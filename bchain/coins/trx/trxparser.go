package trx

import (
	"blockbook/bchain"
	"encoding/hex"
	"math/big"

	"github.com/fbsobreira/gotron/api"
	"github.com/fbsobreira/gotron/common/base58"
	"github.com/fbsobreira/gotron/common/crypto"
	"github.com/fbsobreira/gotron/common/hexutil"
	"github.com/fbsobreira/gotron/core"
	"github.com/fbsobreira/gotron/protocol_util"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/juju/errors"
)

const (
	// TronTypeAddressDescriptorLen fixed length base 58 format
	TronTypeAddressDescriptorLen = crypto.Base58AddressLength
	// TronTypeAddressLen fixed length base 58 format
	TronTypeAddressLen = crypto.AddressLength
	// TronAmountDecimalPoint defines number of decimal points in SUN amounts
	TronAmountDecimalPoint = crypto.AmountDecimalPoint
	// TronTokenIssueAddressDesc respresents address in token issue transactions
	TronTokenIssueAddressDesc = "Token Issue (Account Base)"
	// TronFreezeAddressDesc respresents address in freeze transactions
	TronFreezeAddressDesc = "Freeze (Account Base)"
	// TronGenesisAddress is the address used in genesis block
	TronGenesisAddress = "7YxAaK71utTpYJ8u4Zna7muWxd1pQwimpGxy8"
)

// TronParser handle
type TronParser struct {
	*bchain.BaseParser
}

type completeTransaction struct {
	Tx   *api.TransactionExtention `json:"tx"`
	Info *core.TransactionInfo     `json:"info,omitempty"`
}

// NewTronParser returns new TronParser instance
func NewTronParser(b int) *TronParser {
	return &TronParser{&bchain.BaseParser{
		BlockAddressesToKeep: b,
		AmountDecimalPoint:   crypto.AmountDecimalPoint,
	}}
}

func (p *TronParser) trxTxToTx(tx *api.TransactionExtention, info *core.TransactionInfo, confirmations uint32) (*bchain.Tx, bool, error) {
	txid := tx.GetTxid()

	var (
		fa, ta []string
		vs     *big.Int
		err    error
	)
	assetIssue := false

	if len(tx.GetTransaction().GetRawData().GetContract()) != 1 {
		return nil, false, errors.New("Tx inconsistent")
	}

	contract := tx.GetTransaction().GetRawData().GetContract()[0]

	switch contract.GetType() {
	case core.Transaction_Contract_TransferContract:
		var c core.TransferContract
		if err = ptypes.UnmarshalAny(contract.GetParameter(), &c); err != nil {
			return nil, false, errors.New("Tx inconsistent")
		}
		fa = []string{base58.EncodeCheck(c.GetOwnerAddress())}
		ta = []string{base58.EncodeCheck(c.GetToAddress())}
		vs = new(big.Int).SetInt64(c.Amount)
	case core.Transaction_Contract_TransferAssetContract:
		var c core.TransferAssetContract
		if err = ptypes.UnmarshalAny(contract.GetParameter(), &c); err != nil {
			return nil, false, errors.New("Tx inconsistent")
		}
		fa = []string{base58.EncodeCheck(c.GetOwnerAddress())}
		ta = []string{base58.EncodeCheck(c.GetToAddress())}
		vs = new(big.Int).SetInt64(0) // amount is fetch in get token from TX (c.Amount)
	case core.Transaction_Contract_AssetIssueContract:
		var c core.AssetIssueContract
		if err = ptypes.UnmarshalAny(contract.GetParameter(), &c); err != nil {
			return nil, false, errors.New("Tx inconsistent")
		}
		fa = []string{base58.EncodeCheck(c.GetOwnerAddress())}
		ta = []string{TronTokenIssueAddressDesc}
		vs = new(big.Int).SetInt64(0)
		assetIssue = true
	default:
		return nil, false, errors.New("Transaction type not implemented: " + hexutil.ToHex(txid))

	}

	// TODO: add transactions

	ct := completeTransaction{
		Tx:   tx,
		Info: info,
	}
	hex, _ := proto.Marshal(tx.GetTransaction())
	bbb := &bchain.Tx{
		BlockHeight:   uint32(info.GetBlockNumber()),
		Blocktime:     info.GetBlockTimeStamp() / 1000,
		Confirmations: confirmations,
		// Hex
		// LockTime
		Time: info.GetBlockTimeStamp() / 1000,
		Txid: hexutil.ToHex(txid),
		Vin: []bchain.Vin{
			{
				Addresses: fa,
			},
		},
		Vout: []bchain.Vout{
			{
				N:        0, // there is always up to one To address
				ValueSat: *vs,
				ScriptPubKey: bchain.ScriptPubKey{
					// Hex
					Addresses: ta,
				},
			},
		},
		Hex:              hexutil.ToHex(hex),
		CoinSpecificData: ct,
	}

	return bbb, assetIssue, nil
}

// GetAddrDescFromVout returns internal address representation of given transaction output
func (p *TronParser) GetAddrDescFromVout(output *bchain.Vout) (bchain.AddressDescriptor, error) {
	if len(output.ScriptPubKey.Addresses) != 1 {
		return nil, bchain.ErrAddressMissing
	}
	return p.GetAddrDescFromAddress(output.ScriptPubKey.Addresses[0])
}

func has0xPrefix(s string) bool {
	return len(s) >= 2 && s[0] == '0' && (s[1]|32) == 'x'
}

// GetAddrDescFromAddress returns internal address representation of given address
func (p *TronParser) GetAddrDescFromAddress(address string) (bchain.AddressDescriptor, error) {
	if has0xPrefix(address) {
		address = address[2:]
	}
	if address == TronTokenIssueAddressDesc {
		return []byte(address), nil
	}
	return base58.DecodeCheck(address)
}

// GetAddressesFromAddrDesc returns addresses for given address descriptor with flag if the addresses are searchable
func (p *TronParser) GetAddressesFromAddrDesc(addrDesc bchain.AddressDescriptor) ([]string, bool, error) {
	if addrDesc[0] == 0x41 {
		return []string{base58.EncodeCheck(addrDesc)}, true, nil
	}
	return []string{string(addrDesc)}, false, nil
}

// GetScriptFromAddrDesc returns output script for given address descriptor
func (p *TronParser) GetScriptFromAddrDesc(addrDesc bchain.AddressDescriptor) ([]byte, error) {
	return addrDesc, nil
}

// PackTx packs transaction to byte array
func (p *TronParser) PackTx(tx *bchain.Tx, height uint32, blockTime int64) ([]byte, error) {
	r, ok := tx.CoinSpecificData.(completeTransaction)
	if !ok {
		return nil, errors.New("Missing CoinSpecificData")
	}
	pt := &protocol_util.ProtoCompleteTransaction{}
	pt.Tx = r.Tx
	pt.Info = r.Info
	pt.BlockNumber = uint64(r.Info.GetBlockNumber())
	pt.BlockTime = uint64(r.Info.GetBlockTimeStamp() / 1000)
	return proto.Marshal(pt)
}

// UnpackTx unpacks transaction from byte array
func (p *TronParser) UnpackTx(buf []byte) (*bchain.Tx, uint32, error) {
	var pt protocol_util.ProtoCompleteTransaction
	err := proto.Unmarshal(buf, &pt)
	if err != nil {
		return nil, 0, err
	}

	tx, _, err := p.trxTxToTx(pt.Tx, pt.Info, 0)
	if err != nil {
		return nil, 0, err
	}
	return tx, uint32(pt.BlockNumber), nil
}

// PackedTxidLen returns length in bytes of packed txid
func (p *TronParser) PackedTxidLen() int {
	return 32
}

// PackTxid packs txid to byte array
func (p *TronParser) PackTxid(txid string) ([]byte, error) {
	if has0xPrefix(txid) {
		txid = txid[2:]
	}
	return hex.DecodeString(txid)
}

// UnpackTxid unpacks byte array to txid
func (p *TronParser) UnpackTxid(buf []byte) (string, error) {
	return hexutil.Encode(buf), nil
}

// PackBlockHash packs block hash to byte array
func (p *TronParser) PackBlockHash(hash string) ([]byte, error) {
	if has0xPrefix(hash) {
		hash = hash[2:]
	}
	return hex.DecodeString(hash)
}

// UnpackBlockHash unpacks byte array to block hash
func (p *TronParser) UnpackBlockHash(buf []byte) (string, error) {
	return hexutil.Encode(buf), nil
}

// GetChainType returns TronType
func (p *TronParser) GetChainType() bchain.ChainType {
	return bchain.ChainTronType
}

// GetHeightFromTx returns tron specific data from bchain.Tx
func GetHeightFromTx(tx *bchain.Tx) (uint32, error) {
	csd, ok := tx.CoinSpecificData.(completeTransaction)
	if !ok {
		return 0, errors.New("Missing CoinSpecificData")
	}
	n := csd.Info.GetBlockNumber()
	return uint32(n), nil
}

// TronTypeGetTokensFromTx returns Trc20 data from bchain.Tx
func (p *TronParser) TronTypeGetTokensFromTx(tx *bchain.Tx) ([]bchain.Erc20Transfer, error) {
	var (
		r      []bchain.Erc20Transfer
		err    error
		csd    completeTransaction
		ok     bool
		vs     big.Int
		fa, ta string
	)
	if csd, ok = tx.CoinSpecificData.(completeTransaction); !ok {
		return nil, errors.New("Missing CoinSpecificData")
	}

	if len(csd.Tx.GetTransaction().GetRawData().GetContract()) != 1 {
		return nil, errors.New("Tx inconsistent")
	}

	contract := csd.Tx.GetTransaction().GetRawData().GetContract()[0]

	switch contract.GetType() {
	case core.Transaction_Contract_TransferAssetContract:
		var c core.TransferAssetContract
		if err = ptypes.UnmarshalAny(contract.GetParameter(), &c); err != nil {
			return nil, bchain.ErrTxNotFound
		}
		fa = base58.EncodeCheck(c.GetOwnerAddress())
		ta = base58.EncodeCheck(c.GetToAddress())
		vs.SetInt64(c.Amount)

		r = append(r, bchain.Erc20Transfer{
			Contract: crypto.BytesToAddress(c.GetAssetName()).String(),
			From:     fa,
			To:       ta,
			Tokens:   vs,
		})

	case core.Transaction_Contract_AssetIssueContract:
		var c core.AssetIssueContract
		if err = ptypes.UnmarshalAny(contract.GetParameter(), &c); err != nil {
			return nil, bchain.ErrTxNotFound
		}
		fa = TronTokenIssueAddressDesc
		ta = base58.EncodeCheck(c.GetOwnerAddress())
		vs.SetInt64(c.GetTotalSupply())
		for _, fs := range c.GetFrozenSupply() {
			vs.Sub(&vs, new(big.Int).SetInt64(fs.GetFrozenAmount()))
		}
		r = append(r, bchain.Erc20Transfer{
			Contract: crypto.BytesToAddress([]byte(c.GetId())).String(),
			From:     fa,
			To:       ta,
			Tokens:   vs,
		})
	case core.Transaction_Contract_TriggerSmartContract:
		// TODO: Internal transactions
		if csd.Info.GetLog() != nil {
			r, err = trc20GetTransfersFromLog(csd.Info.GetLog())
			if err != nil {
				return nil, err
			}
		}
	default:
		// Don't need to parse other types
	}
	return r, nil
}

const (
	txStatusUnknown = iota - 3
	txStatusPending
	txStatusRevert
	txStatusFailure
	txStatusOK
)

// TronTxData contains tron specific transaction data
type TronTxData struct {
	Status            int   `json:"status"` // 1 OK, 0 Fail, -1 pending, -2 unknown
	TotalFee          int64 `json:"fees"`
	EnergyUsage       int64 `json:"energyusage"`
	EnergyFee         int64 `json:"energyfee"`
	OriginEnergyUsage int64 `json:"originenergyusage"`
	EnergyUsageTotal  int64 `json:"energyusagetotal"`
	NetUsage          int64 `json:"netusage"`
	NetFee            int64 `json:"netfee"`
}

// GetTronTxData returns TronTxData from bchain.Tx
func GetTronTxData(tx *bchain.Tx) *TronTxData {
	ttd := TronTxData{Status: txStatusUnknown}
	csd, ok := tx.CoinSpecificData.(completeTransaction)
	if ok {
		ttd.TotalFee = csd.Info.GetFee()
		if csd.Info.GetReceipt() != nil {
			ttd.EnergyUsage = csd.Info.GetReceipt().GetEnergyUsage()
			ttd.EnergyFee = csd.Info.GetReceipt().GetEnergyFee()
			ttd.OriginEnergyUsage = csd.Info.GetReceipt().GetOriginEnergyUsage()
			ttd.EnergyUsageTotal = csd.Info.GetReceipt().GetEnergyUsageTotal()
			ttd.NetUsage = csd.Info.GetReceipt().GetNetUsage()
			ttd.NetFee = csd.Info.GetReceipt().GetNetFee()
			switch csd.Info.GetReceipt().GetResult() {
			case core.Transaction_Result_DEFAULT, core.Transaction_Result_SUCCESS:
				ttd.Status = txStatusOK
			case core.Transaction_Result_REVERT:
				ttd.Status = txStatusRevert
			default:
				ttd.Status = txStatusFailure
			}
		}
	}
	return &ttd
}

// EthereumTypeGetErc20FromTx is unsupported
func (p *TronParser) EthereumTypeGetErc20FromTx(tx *bchain.Tx) ([]bchain.Erc20Transfer, error) {
	return nil, errors.New("Not supported")
}
