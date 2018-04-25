package fx

// Currency represents currency as a 3-letter code (ISO 4217).
type Currency string

// Symbol represents the name of a trading instrument.
type Symbol string

// Quote is the price (bid/ask) for a given instrument.
type Quote struct {
	Ask float64
	Bid float64
}

// QuoteFunc returns Quote for a given Symbol. ok is false when a quote is not
// found, otherwise - true.
type QuoteFunc func(symbol Symbol) (q Quote, ok bool)

const commonCurrency Currency = "USD"

// ExchangeRate returns the exchange rate between `from` and `to` currency.
func ExchangeRate(from Currency, to Currency, q QuoteFunc) (float64, bool) {
	if rate, ok := directRate(from, to, q); ok {
		return rate, ok
	}

	// This is a cross rate, try to convert via common currency such as USD.
	if commonCurrency == from || commonCurrency == to {
		return 0, false
	}

	if rate, ok := crossRate(from, to, commonCurrency, q); ok {
		return rate, ok
	}

	return 0, false
}

func directRate(from Currency, to Currency, q QuoteFunc) (float64, bool) {
	if from == to {
		return 1, true
	}

	if quote, ok := q(Symbol(from + to)); ok {
		return (quote.Bid + quote.Ask) / 2, ok
	}

	if quote, ok := q(Symbol(to + from)); ok {
		return ((1 / quote.Bid) + (1 / quote.Ask)) / 2, ok
	}

	return 0, false
}

func crossRate(from Currency, to Currency, common Currency, q QuoteFunc) (float64, bool) {
	r1, ok1 := directRate(from, common, q)
	r2, ok2 := directRate(common, to, q)
	if ok1 && ok2 {
		return r1 * r2, true
	}
	return 0, false
}

// Profit returns the current profit of a position in quote currency. `units` is
// the size of the position in base units, e.g. 2 lots of some instrument with
// contract size 100,000 units, is 200,000 units.
func Profit(side string, openPrice float64, lastPrice Quote, units float64) float64 {
	var priceDiff float64
	if side == "buy" {
		priceDiff = lastPrice.Bid - openPrice
	} else {
		priceDiff = openPrice - lastPrice.Ask
	}

	return priceDiff * units
}
