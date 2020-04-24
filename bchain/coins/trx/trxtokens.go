package trx

import (
	"blockbook/bchain"
	"bytes"
	"context"
	"encoding/hex"
	"math/big"
	"sync"
	"unicode/utf8"

	"github.com/fbsobreira/gotron/common/crypto"
	"github.com/fbsobreira/gotron/common/hexutil"
	"github.com/fbsobreira/gotron/core"
	"github.com/golang/glog"
	"github.com/juju/errors"
)

var trc20abi = `[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"type":"function","signature":"0x06fdde03"},
{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"type":"function","signature":"0x95d89b41"},
{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"type":"function","signature":"0x313ce567"},
{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function","signature":"0x18160ddd"},
{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"type":"function","signature":"0x70a08231"},
{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"success","type":"bool"}],"payable":false,"type":"function","signature":"0xa9059cbb"},
{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"success","type":"bool"}],"payable":false,"type":"function","signature":"0x23b872dd"},
{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"success","type":"bool"}],"payable":false,"type":"function","signature":"0x095ea7b3"},
{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"remaining","type":"uint256"}],"payable":false,"type":"function","signature":"0xdd62ed3e"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"_from","type":"address"},{"indexed":true,"name":"_to","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Transfer","type":"event","signature":"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"_owner","type":"address"},{"indexed":true,"name":"_spender","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Approval","type":"event","signature":"0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"},
{"inputs":[{"name":"_initialAmount","type":"uint256"},{"name":"_tokenName","type":"string"},{"name":"_decimalUnits","type":"uint8"},{"name":"_tokenSymbol","type":"string"}],"payable":false,"type":"constructor"},
{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"},{"name":"_extraData","type":"bytes"}],"name":"approveAndCall","outputs":[{"name":"success","type":"bool"}],"payable":false,"type":"function","signature":"0xcae9ca51"},
{"constant":true,"inputs":[],"name":"version","outputs":[{"name":"","type":"string"}],"payable":false,"type":"function","signature":"0x54fd4d50"}]`

// doing the parsing/processing without using go-tron/accounts/abi library, it is simple to get data from Transfer event
const trc20TransferMtrxodSignature = "0xa9059cbb"
const trc20TransferEventSignature = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
const trc20NameSignature = "0x06fdde03"
const trc20SymbolSignature = "0x95d89b41"
const trc20DecimalsSignature = "0x313ce567"
const trc20BalanceOf = "0x70a08231"

var cachedContracts = make(map[string]*bchain.TronTokenInfo)
var cachedContractsMux sync.Mutex

func addressFromPaddedHex(b []byte) (string, error) {
	return addressFromPaddedHexString(hexutil.ToHex(b))
}

func addressFromPaddedHexString(s string) (string, error) {
	var t big.Int
	var ok bool
	if has0xPrefix(s) {
		_, ok = t.SetString(crypto.AddressPrefixHex+s[2:], 16)
	} else {
		_, ok = t.SetString(crypto.AddressPrefixHex+s, 16)
	}
	if !ok {
		return "", errors.New("Data is not a number")
	}
	a := crypto.BigToAddress(&t)
	return a.String(), nil
}

func trc20GetTransfersFromLog(logs []*core.TransactionInfo_Log) ([]bchain.Erc20Transfer, error) {
	var r []bchain.Erc20Transfer
	for _, l := range logs {
		if len(l.Topics) == 3 && hexutil.ToHex(l.Topics[0]) == trc20TransferEventSignature {
			var t big.Int
			_, ok := t.SetString(hexutil.ToHex(l.Data), 16)
			if !ok {
				return nil, errors.New("Data is not a number")
			}
			from, err := addressFromPaddedHex(l.Topics[1][12:])
			if err != nil {
				return nil, err
			}
			to, err := addressFromPaddedHex(l.Topics[2][12:])
			if err != nil {
				return nil, err
			}
			addr, err := addressFromPaddedHex(l.Address)
			if err != nil {
				return nil, err
			}
			r = append(r, bchain.Erc20Transfer{
				Contract: addr,
				From:     from,
				To:       to,
				Tokens:   t,
			})
		}
	}
	return r, nil
}

func (b *TronRPC) trc20Call(contractDesc bchain.AddressDescriptor, signature string) (string, error) {
	addrBytes := contractDesc
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	data, err := hex.DecodeString(signature)
	if err != nil {
		return "", err
	}
	ct := &core.TriggerSmartContract{
		OwnerAddress:    []byte{0x41, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		ContractAddress: addrBytes,
		Data:            data,
	}
	tx, err := (*b.client).TriggerConstantContract(ctx, ct)
	if err != nil {
		return "", err
	}
	return hexutil.ToHex(tx.GetConstantResult()[0]), nil
}

func (b *TronRPC) trc20GetName(contractDesc bchain.AddressDescriptor) (string, error) {
	data, err := b.trc20Call(contractDesc, trc20NameSignature)
	if err != nil {
		return "", err
	}
	return parseErc20StringProperty(contractDesc, data), nil
}

func (b *TronRPC) trc20GetSymbol(contractDesc bchain.AddressDescriptor) (string, error) {
	data, err := b.trc20Call(contractDesc, trc20SymbolSignature)
	if err != nil {
		return "", err
	}
	return parseErc20StringProperty(contractDesc, data), nil
}

func (b *TronRPC) trc20GetDecimals(contractDesc bchain.AddressDescriptor) (*big.Int, error) {
	data, err := b.trc20Call(contractDesc, trc20DecimalsSignature)
	if err != nil {
		return nil, err
	}
	return parseErc20NumericProperty(contractDesc, data), nil
}

func parseErc20NumericProperty(contractDesc bchain.AddressDescriptor, data string) *big.Int {
	if has0xPrefix(data) {
		data = data[2:]
	}
	if len(data) == 64 {
		var n big.Int
		_, ok := n.SetString(data, 16)
		if ok {
			return &n
		}
	}
	if glog.V(1) {
		glog.Warning("Cannot parse '", data, "' for contract ", contractDesc)
	}
	return nil
}

func parseErc20StringProperty(contractDesc bchain.AddressDescriptor, data string) string {
	if has0xPrefix(data) {
		data = data[2:]
	}
	if len(data) > 128 {
		n := parseErc20NumericProperty(contractDesc, data[64:128])
		if n != nil {
			l := n.Uint64()
			if 2*int(l) <= len(data)-128 {
				b, err := hex.DecodeString(data[128 : 128+2*l])
				if err == nil {
					return string(b)
				}
			}
		}
	} else if len(data) == 64 {
		// allow string properties as 32 bytes of UTF-8 data
		b, err := hex.DecodeString(data)
		if err == nil {
			i := bytes.Index(b, []byte{0})
			if i > 0 {
				b = b[:i]
			}
			if utf8.Valid(b) {
				return string(b)
			}
		}
	}
	if glog.V(1) {
		glog.Warning("Cannot parse '", data, "' for contract ", contractDesc)
	}
	return ""
}

func (b *TronRPC) getTrc10TokenInfo(contractDesc bchain.AddressDescriptor) (*bchain.TronTokenInfo, error) {
	var contract *bchain.TronTokenInfo

	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	bn := new(big.Int).SetBytes(contractDesc)

	asset, err := (*b.client).GetAssetIssueById(ctx, GetMessageBytes(bn.Bytes()))
	if err != nil {
		return nil, err
	}
	if asset != nil {
		contract = &bchain.TronTokenInfo{
			Type:     "TRC10",
			Contract: asset.GetId(),
			Name:     string(asset.GetDescription()),
			Symbol:   string(asset.GetAbbr()),
			Decimals: int(asset.GetPrecision()),
		}
	}
	return contract, nil
}

func (b *TronRPC) getTrc20TokenInfo(contractDesc bchain.AddressDescriptor) (*bchain.TronTokenInfo, error) {
	var contract *bchain.TronTokenInfo
	name, _ := b.trc20GetName(contractDesc)
	if name != "" {
		symbol, err := b.trc20GetSymbol(contractDesc)
		if err != nil {
			return nil, err
		}
		contract = &bchain.TronTokenInfo{
			Type:     "TRC20",
			Contract: contractDesc.String(),
			Name:     name,
			Symbol:   symbol,
		}
		d, err := b.trc20GetDecimals(contractDesc)
		if d != nil {
			contract.Decimals = int(uint8(d.Uint64()))
		} else {
			contract.Decimals = crypto.AmountDecimalPoint
		}
	}
	return contract, nil
}

func isTrc20(contractDesc bchain.AddressDescriptor) bool {
	bn := new(big.Int).SetBytes(contractDesc)
	if bn.Cmp(new(big.Int).SetBytes(crypto.HexToAddress("410000000000000000000000000000000000000000").Bytes())) < 0 {
		return false
	}
	return true
}

// TronTypeGetTokenInfo returns information about TRC20 contract
func (b *TronRPC) TronTypeGetTokenInfo(contractDesc bchain.AddressDescriptor) (*bchain.TronTokenInfo, error) {
	cds := string(contractDesc)
	cachedContractsMux.Lock()
	contract, found := cachedContracts[cds]
	cachedContractsMux.Unlock()
	if !found {
		var err error
		if glog.V(2) {
			glog.Info("Getting token info: ", len(contractDesc), contractDesc)
		}
		if isTrc20(contractDesc) {
			contract, err = b.getTrc20TokenInfo(contractDesc)
		} else {
			contract, err = b.getTrc10TokenInfo(contractDesc)
		}
		if err != nil {
			return nil, err
		}
		cachedContractsMux.Lock()
		cachedContracts[cds] = contract
		cachedContractsMux.Unlock()
	}
	return contract, nil
}

// TronTypeGetTrc20ContractBalance returns balance of TRC20 contract for given address
func (b *TronRPC) TronTypeGetTrc20ContractBalance(addrDesc, contractDesc bchain.AddressDescriptor) (*big.Int, error) {
	if !isTrc20(contractDesc) {
		return nil, errors.New("Not valid TRC20 address")
	}
	addr := addrDesc.String()
	req := trc20BalanceOf + "0000000000000000000000000000000000000000000000000000000000000000"[len(addr)-2:] + addr[2:]
	data, err := b.trc20Call(contractDesc, req)
	if err != nil {
		return nil, err
	}
	r := parseErc20NumericProperty(contractDesc, data)
	if r == nil {
		return nil, errors.New("Invalid balance")
	}
	return r, nil
}
