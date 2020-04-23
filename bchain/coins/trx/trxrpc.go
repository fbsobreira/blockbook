package trx

import (
	"blockbook/bchain"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strconv"
	"sync"
	"time"

	"github.com/fbsobreira/gotron/api"
	"github.com/fbsobreira/gotron/common/crypto"
	"github.com/fbsobreira/gotron/common/hexutil"
	"github.com/fbsobreira/gotron/core"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/juju/errors"
	"google.golang.org/grpc"
)

// TronNet type specifies the type of tron network
type TronNet uint32

const (
	// MainNet is production network
	MainNet TronNet = 1
	// TestNet is shasta test network
	TestNet TronNet = 3
)

// Configuration represents json config file
type Configuration struct {
	CoinName                    string `json:"coin_name"`
	CoinShortcut                string `json:"coin_shortcut"`
	RPCURL                      string `json:"rpc_url"`
	RPCTimeout                  int    `json:"rpc_timeout"`
	BlockAddressesToKeep        int    `json:"block_addresses_to_keep"`
	MempoolTxTimeoutHours       int    `json:"mempoolTxTimeoutHours"`
	QueryBackendOnMempoolResync bool   `json:"queryBackendOnMempoolResync"`
}

// TronRPC is an interface to JSON-RPC trx service.
type TronRPC struct {
	*bchain.BaseChain
	rpc                  *grpc.ClientConn
	client               *api.WalletClient
	timeout              time.Duration
	Parser               *TronParser
	Mempool              *bchain.MempoolEthereumType
	mempoolInitialized   bool
	bestHeaderLock       sync.Mutex
	bestHeader           *api.BlockExtention
	bestHeaderTime       time.Time
	chanNewBlock         chan *api.BlockExtention
	newBlockSubscription *ClientSubscription
	chanNewTx            chan crypto.Hash
	ChainConfig          *Configuration
	genesisTx            map[string]*bchain.Tx
}

// NewTronRPC returns new TrxRPC instance.
func NewTronRPC(config json.RawMessage, pushHandler func(bchain.NotificationType)) (bchain.BlockChain, error) {
	var err error
	var c Configuration
	err = json.Unmarshal(config, &c)
	if err != nil {
		return nil, errors.Annotatef(err, "Invalid configuration file")
	}
	// keep at least 30 mappings block->addresses to allow rollback
	if c.BlockAddressesToKeep < 30 {
		c.BlockAddressesToKeep = 30
	}

	rc, ec, err := openRPC(c.RPCURL)
	if err != nil {
		return nil, err
	}

	s := &TronRPC{
		BaseChain:   &bchain.BaseChain{},
		client:      ec,
		rpc:         rc,
		ChainConfig: &c,
	}

	// always create parser
	s.Parser = NewTronParser(c.BlockAddressesToKeep)
	s.timeout = time.Duration(c.RPCTimeout) * time.Second

	// new blocks notifications handling
	// the subscription is done in Initialize
	s.chanNewBlock = make(chan *api.BlockExtention)
	go func() {
		for {
			h, ok := <-s.chanNewBlock
			if !ok {
				break
			}
			glog.V(2).Info("grpc: new block header ", h.BlockHeader.GetRawData().GetNumber())
			// update best header to the new header
			s.bestHeaderLock.Lock()
			s.bestHeader = h
			s.bestHeaderTime = time.Now()
			s.bestHeaderLock.Unlock()
			// notify blockbook
			pushHandler(bchain.NotificationNewBlock)
		}
	}()

	// new mempool transaction notifications handling
	// the subscription is done in Initialize
	s.chanNewTx = make(chan crypto.Hash)
	go func() {
		for {
			t, ok := <-s.chanNewTx
			if !ok {
				break
			}
			hex := t.Hex()
			if glog.V(2) {
				glog.Info("grpc: new tx ", hex)
			}
			s.Mempool.AddTransactionToMempool(hex)
			pushHandler(bchain.NotificationNewTx)
		}
	}()

	return s, nil
}

func openRPC(url string) (*grpc.ClientConn, *api.WalletClient, error) {

	rc, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	tc := api.NewWalletClient(rc)
	return rc, &tc, nil
}

// Initialize initializes tron rpc interface
func (b *TronRPC) Initialize() error {
	id := 1
	// parameters for getInfo request
	switch TronNet(id) {
	case MainNet:
		b.Testnet = false
		b.Network = "livenet"
	case TestNet:
		b.Testnet = true
		b.Network = "testnet"
	default:
		return errors.Errorf("Unknown network id %v", id)
	}
	glog.Info("grpc: block chain ", b.Network)

	return b.loadGenesisTx()
}

func (b *TronRPC) loadGenesisTx() error {

	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	h, err := (*b.client).GetBlockByNum2(ctx, GetMessageNumber(0))
	if err != nil {
		return err
	}
	b.genesisTx = make(map[string]*bchain.Tx)
	transactions := h.GetTransactions()
	for i := range transactions {
		tx := transactions[i]
		info := core.TransactionInfo{
			Fee:            0,
			BlockNumber:    0,
			BlockTimeStamp: tx.GetTransaction().GetRawData().GetTimestamp(),
		}
		btx, assetIssue, err := b.Parser.trxTxToTx(tx, &info, 0)
		if err != nil {
			return err
		}
		if assetIssue {
			if err := b.updateAssetIssueID(btx); err != nil {
				return err
			}
		}
		glog.Info("Adding TX to genesis: ", hexutil.Encode(tx.GetTxid()))
		b.genesisTx[string(tx.GetTxid())] = btx
	}
	return nil
}

// CreateMempool creates mempool if not already created, however does not initialize it
func (b *TronRPC) CreateMempool(chain bchain.BlockChain) (bchain.Mempool, error) {
	if b.Mempool == nil {
		b.Mempool = bchain.NewMempoolEthereumType(chain, b.ChainConfig.MempoolTxTimeoutHours, b.ChainConfig.QueryBackendOnMempoolResync)
		glog.Info("mempool created, MempoolTxTimeoutHours=", b.ChainConfig.MempoolTxTimeoutHours, ", QueryBackendOnMempoolResync=", b.ChainConfig.QueryBackendOnMempoolResync)
	}
	return b.Mempool, nil
}

// InitializeMempool creates subscriptions to newHeads and newPendingTransactions
func (b *TronRPC) InitializeMempool(addrDescForOutpoint bchain.AddrDescForOutpointFunc, onNewTxAddr bchain.OnNewTxAddrFunc) error {
	if b.Mempool == nil {
		return errors.New("Mempool not created")
	}

	// get initial mempool transactions
	txs, err := b.GetMempoolTransactions()
	if err != nil {
		return err
	}
	for _, txid := range txs {
		b.Mempool.AddTransactionToMempool(txid)
	}

	b.Mempool.OnNewTxAddr = onNewTxAddr

	if err = b.subscribeEvents(); err != nil {
		return err
	}

	b.mempoolInitialized = true

	return nil
}

func (b *TronRPC) subscribeEvents() error {

	return nil
}

func (b *TronRPC) closeRPC() {
	if b.newBlockSubscription != nil {
		b.newBlockSubscription.Unsubscribe()
	}

	if b.rpc != nil {
		b.rpc.Close()
	}
}

func (b *TronRPC) reconnectRPC() error {
	glog.Info("Reconnecting RPC")
	b.closeRPC()
	rc, tc, err := openRPC(b.ChainConfig.RPCURL)
	if err != nil {
		return err
	}
	b.rpc = rc
	b.client = tc
	return b.subscribeEvents()
}

// Shutdown cleans up rpc interface to tron
func (b *TronRPC) Shutdown(ctx context.Context) error {
	b.closeRPC()
	close(b.chanNewBlock)
	glog.Info("grpc: shutdown")
	return nil
}

// GetCoinName returns coin name
func (b *TronRPC) GetCoinName() string {
	return b.ChainConfig.CoinName
}

// GetSubversion returns empty string, tron does not have subversion
func (b *TronRPC) GetSubversion() string {
	return ""
}

// GetChainInfo returns information about the connected backend
func (b *TronRPC) GetChainInfo() (*bchain.ChainInfo, error) {
	h, err := b.getBestHeader()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	nodeInfo, err := (*b.client).GetNodeInfo(ctx, new(api.EmptyMessage))
	if err != nil {
		return nil, err
	}

	rv := &bchain.ChainInfo{
		Blocks:          int(h.GetBlockHeader().GetRawData().GetNumber()),
		Bestblockhash:   hexutil.Encode(h.GetBlockid()),
		Difficulty:      "",
		Version:         nodeInfo.GetConfigNodeInfo().GetCodeVersion(),
		ProtocolVersion: nodeInfo.GetConfigNodeInfo().GetP2PVersion(),
	}
	id := 1
	if id == 1 {
		rv.Chain = "mainnet"
	} else {
		rv.Chain = "testnet " + strconv.Itoa(id)
	}
	return rv, nil
}

func (b *TronRPC) getBestHeader() (*api.BlockExtention, error) {
	b.bestHeaderLock.Lock()
	defer b.bestHeaderLock.Unlock()
	// if the best header was not updated for 1 minutes, there could be a subscription problem, reconnect RPC
	// do it only in case of normal operation, not initial synchronization
	if b.bestHeaderTime.Add(1*time.Minute).Before(time.Now()) && !b.bestHeaderTime.IsZero() && b.mempoolInitialized {
		err := b.reconnectRPC()
		if err != nil {
			return nil, err
		}
		b.bestHeader = nil
	}
	if b.bestHeader == nil {
		var err error
		ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
		defer cancel()
		b.bestHeader, err = (*b.client).GetNowBlock2(ctx, new(api.EmptyMessage))
		if err != nil {
			b.bestHeader = nil
			return nil, err
		}
		b.bestHeaderTime = time.Now()
	}
	return b.bestHeader, nil
}

// GetBestBlockHash returns hash of the tip of the best-block-chain
func (b *TronRPC) GetBestBlockHash() (string, error) {
	h, err := b.getBestHeader()
	if err != nil {
		return "", err
	}
	return hexutil.Encode(h.GetBlockid()), nil
}

// GetBestBlockHeight returns height of the tip of the best-block-chain
func (b *TronRPC) GetBestBlockHeight() (uint32, error) {
	h, err := b.getBestHeader()
	if err != nil {
		return 0, err
	}
	return uint32(h.GetBlockHeader().GetRawData().GetNumber()), nil
}

// GetBlockByID returns block extension by hash
func (b *TronRPC) GetBlockByID(hash string) (*api.BlockExtention, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	id := crypto.HexToHash(hash)

	block, err := (*b.client).GetBlockById(ctx, GetMessageBytes(id.Bytes()))
	if err != nil {
		return nil, err
	}

	h, err := (*b.client).GetBlockByNum2(ctx, GetMessageNumber(block.GetBlockHeader().GetRawData().GetNumber()))
	if err != nil {
		return nil, err
	}
	return h, nil
}

// GetBlockHash returns hash of block in best-block-chain at given height
func (b *TronRPC) GetBlockHash(height uint32) (string, error) {

	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	h, err := (*b.client).GetBlockByNum2(ctx, GetMessageNumber(int64(height)))
	if err != nil {
		return "", errors.Annotatef(err, "height %v", height)
	}
	return hexutil.Encode(h.GetBlockid()), nil
}

func (b *TronRPC) trxHeaderToBlockHeader(h *api.BlockExtention) (*bchain.BlockHeader, error) {
	height := h.GetBlockHeader().GetRawData().GetNumber()
	if height < 0 {
		return nil, bchain.ErrBlockNotFound
	}
	c, err := b.computeConfirmations(uint64(height))
	if err != nil {
		return nil, err
	}
	time := h.GetBlockHeader().GetRawData().GetTimestamp() / 1000
	if height > 0 && time <= 0 {
		return nil, bchain.ErrBlockNotFound
	}
	size := proto.Size(h)

	return &bchain.BlockHeader{
		Hash:          hexutil.ToHex(h.GetBlockid()),
		Prev:          hexutil.ToHex(h.GetBlockHeader().GetRawData().GetParentHash()),
		Height:        uint32(height),
		Confirmations: int(c),
		Time:          time,
		Size:          int(size),
	}, nil
}

// GetBlockHeader returns header of block with given hash
func (b *TronRPC) GetBlockHeader(hash string) (*bchain.BlockHeader, error) {
	h, err := b.GetBlockByID(hash)
	if err != nil {
		return nil, err
	}
	return b.trxHeaderToBlockHeader(h)
}

func (b *TronRPC) computeConfirmations(n uint64) (uint32, error) {
	bh, err := b.getBestHeader()
	if err != nil {
		return 0, err
	}
	bn := bh.GetBlockHeader().GetRawData().GetNumber()
	// transaction in the best block has 1 confirmation
	return uint32(uint64(bn) - n + 1), nil
}

// GetTransactionInfoByIDString return event logs
func (b *TronRPC) GetTransactionInfoByIDString(hash string) (*core.TransactionInfo, error) {
	return b.GetTransactionInfoByID(crypto.HexToHash(hash).Bytes())
}

// GetTransactionInfoByID return event logs
func (b *TronRPC) GetTransactionInfoByID(hash []byte) (*core.TransactionInfo, error) {
	transactionID := new(api.BytesMessage)
	transactionID.Value = hash

	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	Info, err := (*b.client).GetTransactionInfoById(ctx, transactionID)
	if err != nil {
		return nil, err
	}
	return Info, nil

}

// GetTransactionByID return event logs
func (b *TronRPC) GetTransactionByID(hash []byte) (*api.TransactionExtention, error) {
	txE := new(api.TransactionExtention)

	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	tx, err := (*b.client).GetTransactionById(ctx, GetMessageBytes(hash))
	if err != nil {
		return nil, err
	}
	txE.Transaction = tx
	txE.Txid = hash
	return txE, nil
}

// GetBlock returns block with given hash or height, hash has precedence if both passed
func (b *TronRPC) GetBlock(hash string, height uint32) (*bchain.Block, error) {
	var err error
	var h *api.BlockExtention
	if len(hash) > 0 {
		h, err = b.GetBlockByID(hash)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
		defer cancel()
		h, err = (*b.client).GetBlockByNum2(ctx, GetMessageNumber(int64(height)))
	}
	if err != nil {
		return nil, err
	}
	if proto.Size(h) == 0 {
		return nil, bchain.ErrBlockNotFound
	}

	bbh, err := b.trxHeaderToBlockHeader(h)
	if err != nil {
		return nil, errors.Annotatef(err, "hash %v, height %v", hash, height)
	}

	transactions := h.GetTransactions()
	btxs := make([]bchain.Tx, len(transactions))
	for i := range transactions {
		tx := transactions[i]
		info, err := b.GetTransactionInfoByID(tx.GetTxid())
		if err != nil {
			return nil, errors.Annotatef(err, "hash %v, height %v, txid %v", hash, height, tx.GetTxid())
		}
		btx, assetIssue, err := b.Parser.trxTxToTx(tx, info, uint32(bbh.Confirmations))
		if err != nil {
			return nil, errors.Annotatef(err, "hash %v, height %v, txid %v", hash, height, tx.GetTxid())
		}
		if assetIssue {
			if err := b.updateAssetIssueID(btx); err != nil {
				return nil, errors.Annotatef(err, "hash %v, height %v, txid %v", hash, height, tx.GetTxid())
			}
		}

		btxs[i] = *btx
		if b.mempoolInitialized {
			b.Mempool.RemoveTransactionFromMempool(hexutil.ToHex(tx.GetTxid()))
		}
	}
	bbk := bchain.Block{
		BlockHeader: *bbh,
		Txs:         btxs,
	}
	return &bbk, nil
}

func getHashes(txs []*api.TransactionExtention) []string {
	txids := make([]string, len(txs))
	for i, tx := range txs {
		txids[i] = hexutil.ToHex(tx.GetTxid())
	}
	return txids
}

// GetBlockInfo returns extended header (more info than in bchain.BlockHeader) with a list of txids
func (b *TronRPC) GetBlockInfo(hash string) (*bchain.BlockInfo, error) {
	h, err := b.GetBlockByID(hash)
	if err != nil {
		return nil, err
	}

	bch, err := b.trxHeaderToBlockHeader(h)
	if err != nil {
		return nil, err
	}

	return &bchain.BlockInfo{
		BlockHeader: *bch,
		MerkleRoot:  hexutil.ToHex(h.GetBlockHeader().GetRawData().GetTxTrieRoot()),
		Version: json.Number(
			strconv.FormatUint(uint64(h.GetBlockHeader().GetRawData().GetVersion()),
				10)),
		Txids: getHashes(h.GetTransactions()),
	}, nil
}

// GetTransactionForMempool returns a transaction by the transaction ID.
// It could be optimized for mempool, i.e. without block time and confirmations
func (b *TronRPC) GetTransactionForMempool(txid string) (*bchain.Tx, error) {
	return b.GetTransaction(txid)
}

// GetTransaction returns a transaction by the transaction ID.
func (b *TronRPC) GetTransaction(txid string) (*bchain.Tx, error) {
	hash := crypto.HexToHash(txid)
	tx, err := b.GetTransactionByID(hash.Bytes())
	if err != nil {
		return nil, err
	}
	info, err := b.GetTransactionInfoByID(hash.Bytes())
	if err != nil {
		return nil, err
	}
	// Check if transaction info exists
	if bytes.Compare(hash.Bytes(), info.GetId()) != 0 {
		// Check if its a genesis transaction
		gTx := b.genesisTx[string(hash.Bytes())]
		if gTx != nil {
			confirmation, _ := b.computeConfirmations(uint64(gTx.BlockHeight))
			gTx.Confirmations = confirmation
			return gTx, nil
		}
		return nil, bchain.ErrTxNotFound
	}

	confirmation, _ := b.computeConfirmations(uint64(info.GetBlockNumber()))
	btx, assetIssue, err := b.Parser.trxTxToTx(tx, info, confirmation)
	if err != nil {
		return nil, errors.Annotatef(err, "txid %v", txid)
	}
	if assetIssue {
		if err := b.updateAssetIssueID(btx); err != nil {
			return nil, errors.Annotatef(err, "txid %v", txid)
		}
	}

	return btx, nil
}

func (b *TronRPC) updateAssetIssueID(btx *bchain.Tx) error {
	ct := btx.CoinSpecificData.(completeTransaction)
	contract := ct.Tx.GetTransaction().GetRawData().GetContract()
	if len(contract) != 1 {
		return errors.New("Tx inconsistent")
	}

	var c core.AssetIssueContract
	if err := ptypes.UnmarshalAny(contract[0].GetParameter(), &c); err != nil {
		return errors.New("Tx inconsistent")
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	ai, err := (*b.client).GetAssetIssueByAccount(ctx, &core.Account{
		Address: c.GetOwnerAddress(),
	})
	if err != nil {
		return errors.New("Tx inconsistent")
	}

	aic := ai.GetAssetIssue()
	if len(aic) != 1 {
		return errors.New("Tx inconsistent")
	}

	if contract[0].Parameter, err = ptypes.MarshalAny(aic[0]); err != nil {
		return errors.New("Tx inconsistent")
	}
	btx.CoinSpecificData = ct

	return nil
}

// GetTransactionSpecific returns json as returned by backend, with all coin specific data
func (b *TronRPC) GetTransactionSpecific(tx *bchain.Tx) (json.RawMessage, error) {
	csd, ok := tx.CoinSpecificData.(completeTransaction)
	if !ok {
		ntx, err := b.GetTransaction(tx.Txid)
		if err != nil {
			return nil, err
		}
		csd, ok = ntx.CoinSpecificData.(completeTransaction)
		if !ok {
			return nil, errors.New("Cannot get CoinSpecificData")
		}
	}
	m, err := json.Marshal(&csd)
	return json.RawMessage(m), err
}

// GetMempoolTransactions returns transactions in mempool
func (b *TronRPC) GetMempoolTransactions() ([]string, error) {

	return []string{}, nil
}

// EstimateFee returns fee estimation
func (b *TronRPC) EstimateFee(blocks int) (big.Int, error) {
	return b.EstimateSmartFee(blocks, true)
}

// EstimateSmartFee returns fee estimation
func (b *TronRPC) EstimateSmartFee(blocks int, conservative bool) (big.Int, error) {
	r := new(big.Int).SetInt64(10)
	return *r, nil
}

func getStringFromMap(p string, params map[string]interface{}) (string, bool) {
	v, ok := params[p]
	if ok {
		s, ok := v.(string)
		return s, ok
	}
	return "", false
}

// SendRawTransaction sends raw transaction
func (b *TronRPC) SendRawTransaction(txHex string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	buf, err := hex.DecodeString(txHex)
	if err != nil {
		return "", err
	}
	var tx core.Transaction
	proto.Unmarshal(buf, &tx)
	result, err := (*b.client).BroadcastTransaction(ctx, &tx)
	if err != nil {
		return "", err
	} else if result.GetCode() != api.Return_SUCCESS {
		return "", errors.New("SendRawTransaction: failed. " + hexutil.ToHex(result.GetMessage()))
	}
	return hexutil.ToHex(result.GetMessage()), nil
}

// TronTypeGetBalance returns current balance of an address
func (b *TronRPC) TronTypeGetBalance(addrDesc bchain.AddressDescriptor) (*big.Int, map[string]int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	account := new(core.Account)
	account.Address = addrDesc

	acc, err := (*b.client).GetAccount(ctx, account)
	if err != nil {
		return nil, nil, err
	}
	balance := new(big.Int).SetInt64(acc.GetBalance())
	return balance, acc.GetAssetV2(), nil
}

// GetChainParser returns tron BlockChainParser
func (b *TronRPC) GetChainParser() bchain.BlockChainParser {
	return b.Parser
}

// GetMessageBytes return grpc message from bytes
func GetMessageBytes(m []byte) *api.BytesMessage {
	message := new(api.BytesMessage)
	message.Value = m
	return message
}

// GetMessageNumber return grpc message number
func GetMessageNumber(n int64) *api.NumberMessage {
	message := new(api.NumberMessage)
	message.Num = n
	return message
}

// TronTypeEstimateFee is not supported
func (b *TronRPC) TronTypeEstimateFee(tx string) (uint64, error) {
	if has0xPrefix(tx) {
		return uint64(len(tx[2:]) / 2), nil
	}
	return uint64(len(tx) / 2), nil
}
