package intrinio

import (
	"encoding/binary"
	"log"
	"math"
	"sync"
)

type EquityTrade struct {
	Symbol       string
	Source       uint8
	MarketCenter rune
	Price        float32
	Size         uint32
	TotalVolume  uint32
	Timestamp    float64
	Conditions   string
}

func parseEquityTrade(bytes []byte) EquityTrade {
	symbolLen := bytes[2]
	symbol := string(bytes[3 : 3+symbolLen])
	source := bytes[3+symbolLen]
	marketCenter := rune(binary.LittleEndian.Uint16(bytes[4+symbolLen : 6+symbolLen]))
	price := math.Float32frombits(binary.LittleEndian.Uint32(bytes[6+symbolLen : 10+symbolLen]))
	size := binary.LittleEndian.Uint32(bytes[10+symbolLen : 14+symbolLen])
	timestamp := float64(binary.LittleEndian.Uint64(bytes[14+symbolLen:22+symbolLen])) / 1000000000.0
	totalVolume := binary.LittleEndian.Uint32(bytes[22+symbolLen : 26+symbolLen])
	conditionsLen := bytes[26+symbolLen]
	conditions := ""
	if conditionsLen > 0 {
		conditions = string(bytes[27+symbolLen : 27+symbolLen+conditionsLen])
	}
	return EquityTrade{
		Symbol:       symbol,
		Source:       source,
		MarketCenter: marketCenter,
		Price:        price,
		Size:         size,
		Timestamp:    timestamp,
		TotalVolume:  totalVolume,
		Conditions:   conditions,
	}
}

type QuoteType uint8

const (
	ASK QuoteType = 1
	BID QuoteType = 2
)

type EquityQuote struct {
	Type         QuoteType
	Symbol       string
	Source       uint8
	MarketCenter rune
	Price        float32
	Size         uint32
	Timestamp    float64
	Conditions   string
}

func parseEquityQuote(bytes []byte) EquityQuote {
	symbolLen := bytes[2]
	symbol := string(bytes[3 : 3+symbolLen])
	source := bytes[3+symbolLen]
	marketCenter := rune(binary.LittleEndian.Uint16(bytes[4+symbolLen : 6+symbolLen]))
	price := math.Float32frombits(binary.LittleEndian.Uint32(bytes[6+symbolLen : 10+symbolLen]))
	size := binary.LittleEndian.Uint32(bytes[10+symbolLen : 14+symbolLen])
	timestamp := float64(binary.LittleEndian.Uint64(bytes[14+symbolLen:22+symbolLen])) / 1000000000.0
	conditionsLen := bytes[22+symbolLen]
	conditions := ""
	if conditionsLen > 0 {
		conditions = string(bytes[23+symbolLen : 23+symbolLen+conditionsLen])
	}
	return EquityQuote{
		Type:         QuoteType(bytes[0]),
		Symbol:       symbol,
		Source:       source,
		MarketCenter: marketCenter,
		Price:        price,
		Size:         size,
		Timestamp:    timestamp,
		Conditions:   conditions,
	}
}

func workOnEquities(
	readChannel <-chan []byte,
	onTrade func(EquityTrade),
	onQuote func(EquityQuote),
	onCandle func(EquityCandle)) {

	aggregator := NewTradeAggregator()
	lastCandleTime := float64(0)

	select {
	case data := <-readChannel:
		count := data[0]
		startIndex := 1
		for i := 0; i < int(count); i++ {
			msgType := data[startIndex]
			if (msgType == 1) || (msgType == 2) {
				endIndex := startIndex + int(data[startIndex+1])
				quote := parseEquityQuote(data[startIndex:endIndex])
				startIndex = endIndex
				if onQuote != nil {
					onQuote(quote)
				}
			} else if msgType == 0 {
				endIndex := startIndex + int(data[startIndex+1])
				trade := parseEquityTrade(data[startIndex:endIndex])
				startIndex = endIndex

				// Add trade to aggregator
				aggregator.AddTrade(trade)

				// Check if we need to emit candles
				currentMinute := math.Floor(trade.Timestamp/60) * 60
				if currentMinute > lastCandleTime {
					candles := aggregator.GetAndClearCandles(currentMinute)
					for _, candle := range candles {
						if onCandle != nil {
							onCandle(candle)
						}
					}
					lastCandleTime = currentMinute
				}

				if onTrade != nil {
					onTrade(trade)
				}
			} else {
				log.Printf("Equity Client - Invalid message type: %d", msgType)
			}
		}
	default:
	}
}

func composeEquityJoinMsg(
	useTrade bool,
	useQuote bool,
	symbol string) []byte {
	var tradesOnly uint8 = 0
	if !useQuote {
		tradesOnly = 1
	}
	message := make([]byte, 0, 11)
	message = append(message, 74, tradesOnly)
	message = append(message, []byte(symbol)...)
	log.Printf("Equity Client - Composed join msg for channel %s\n", symbol)
	return message
}

func composeEquityLeaveMsg(symbol string) []byte {
	message := make([]byte, 0, 10)
	message = append(message, 76)
	message = append(message, []byte(symbol)...)
	log.Printf("Equity Client - Composed leave msg for channel %s\n", symbol)
	return message
}

type EquityCandle struct {
	Symbol    string
	Open      float32
	High      float32
	Low       float32
	Close     float32
	Volume    uint32
	Timestamp float64
}

type TradeAggregator struct {
	trades map[string][]EquityTrade
	mu     sync.RWMutex
}

func NewTradeAggregator() *TradeAggregator {
	return &TradeAggregator{
		trades: make(map[string][]EquityTrade),
	}
}

func (ta *TradeAggregator) AddTrade(trade EquityTrade) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	ta.trades[trade.Symbol] = append(ta.trades[trade.Symbol], trade)
}

func (ta *TradeAggregator) GetAndClearCandles(currentTime float64) map[string]EquityCandle {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	candles := make(map[string]EquityCandle)

	for symbol, trades := range ta.trades {
		if len(trades) == 0 {
			continue
		}

		// Find trades that belong to the previous minute
		var minuteTrades []EquityTrade
		for _, trade := range trades {
			if trade.Timestamp < currentTime && trade.Timestamp >= currentTime-60 {
				minuteTrades = append(minuteTrades, trade)
			}
		}

		if len(minuteTrades) > 0 {
			candle := EquityCandle{
				Symbol:    symbol,
				Open:      minuteTrades[0].Price,
				High:      minuteTrades[0].Price,
				Low:       minuteTrades[0].Price,
				Close:     minuteTrades[len(minuteTrades)-1].Price,
				Volume:    0,
				Timestamp: currentTime - 60,
			}

			for _, trade := range minuteTrades {
				if trade.Price > candle.High {
					candle.High = trade.Price
				}
				if trade.Price < candle.Low {
					candle.Low = trade.Price
				}
				candle.Volume += trade.Size
			}

			candles[symbol] = candle
		}

		// Keep only trades from the current minute
		var remainingTrades []EquityTrade
		for _, trade := range trades {
			if trade.Timestamp >= currentTime-60 {
				remainingTrades = append(remainingTrades, trade)
			}
		}
		ta.trades[symbol] = remainingTrades
	}

	return candles
}
