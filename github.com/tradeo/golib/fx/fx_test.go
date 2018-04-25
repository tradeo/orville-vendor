package fx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExchangeRate(t *testing.T) {
	quotes := map[Symbol]Quote{
		"EURUSD": {Bid: 1.10, Ask: 1.12},
		"USDJPY": {Bid: 1.20, Ask: 1.22},
		"GBPUSD": {Bid: 1.30, Ask: 1.32},
		"USDCHF": {Bid: 1.40, Ask: 1.42},
		"USDCAD": {Bid: 1.50, Ask: 1.52},
		"AAPL":   {Bid: 1.70, Ask: 1.72},
	}

	quoteFunc := func(symbol Symbol) (Quote, bool) {
		quote, ok := quotes[symbol]
		return quote, ok
	}

	EURUSD := (1.10 + 1.12) / 2
	USDJPY := (1.20 + 1.22) / 2
	USDCHF := (1.40 + 1.42) / 2

	USDEUR := (1/1.10 + 1/1.12) / 2
	JPYUSD := (1/1.20 + 1/1.22) / 2
	CHFUSD := (1/1.40 + 1/1.42) / 2

	tests := []struct {
		from Currency
		to   Currency
		out  float64
	}{
		{"USD", "USD", 1.0},
		{"EUR", "USD", EURUSD},
		{"USD", "EUR", USDEUR},
		{"JPY", "USD", JPYUSD},
		{"EUR", "JPY", EURUSD * USDJPY},
		{"CHF", "JPY", CHFUSD * USDJPY},
		{"JPY", "EUR", USDEUR * JPYUSD},
		{"JPY", "CHF", USDCHF * JPYUSD},
	}

	for _, tt := range tests {
		rate, ok := ExchangeRate(tt.from, tt.to, quoteFunc)
		assert.True(t, ok)
		assert.InEpsilon(t, tt.out, rate, 0.00001)
	}

	_, ok := ExchangeRate("ABC", "DEF", quoteFunc)
	assert.False(t, ok)
}

func TestProfit(t *testing.T) {
	quote := Quote{Bid: 1.10, Ask: 1.12}

	profit := Profit("buy", 1.05, quote, 200000.0)
	assert.InEpsilon(t, (1.10-1.05)*200000, profit, 0.0001)

	profit = Profit("sell", 1.05, quote, 200000.0)
	assert.InEpsilon(t, (1.05-1.12)*200000, profit, 0.0001)
}
