const WebSocket = require('ws');
const axios = require('axios');

function connectWebSocket() {
    const marketType = process.argv[2];
    const tradingPair = process.argv[3];

    let wsUrl, subscription;
    if (marketType.toLowerCase() === 'spot') {
        wsUrl = 'wss://ws.kraken.com/';
        subscription = {
            event: 'subscribe',
            pair: [tradingPair],
            subscription: { name: 'trade' }
        };
    }

    console.log('Attempting to connect...');
    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log(`WebSocket connection opened for Kraken: ${tradingPair} (${marketType})`);  // Log: Connection successful

        const subscription = {
            event: 'subscribe',
            pair: [tradingPair],
            subscription: { name: 'trade' }
        };
        ws.send(JSON.stringify(subscription));
        ws.ping();
    });

    ws.on('message', async (data) => {
    const jsonData = JSON.parse(data);
    console.log('WebSocket Message:', jsonData);

    // Check if jsonData is in the expected format
    if (Array.isArray(jsonData) && jsonData.length > 1 && Array.isArray(jsonData[1])) {
        // Now, jsonData[1] is an array of trades, so we'll process each trade individually
        for (let trade of jsonData[1]) {  // iterating over each trade
            const filteredData = filterData(trade);  // processing the individual trade
            console.log('Received trade data:', filteredData);

            // The rest of your logic stays the same, but it's now inside the loop,
            // so each trade gets processed and sent to your server.
            const encodedMessage = encodeMessage(tradingPair, filteredData);

            try {
                await axios.post('http://localhost:3030/logdata', encodedMessage, {
                    headers: { 'Content-Type': 'application/json' }
                });
            } catch (error) {
                console.error('Failed to send data to server:', error);
            }
        }
    }
});


    ws.on('close', (code, reason) => {
        console.log(`WebSocket connection closed. Code: ${code}, Reason: ${reason}`);
        setTimeout(() => {
            console.log('Kraken reconnecting...');
            connectWebSocket();
        }, 1000);
    });

    ws.on('error', (error) => {
        console.error(`WebSocket encountered error for Kraken: ${tradingPair}:`, error);
        ws.close();
    });
}

(async () => {
    connectWebSocket();
})();

function filterData(data) {
  console.log(data);
  // Extract the timestamp and convert to milliseconds (from seconds)
  let timestamp = Number(data[2]) * 1000; // Convert from seconds to milliseconds

  // Now, we ensure it's an integer to discard any extra precision
  timestamp = Math.floor(timestamp); // This removes any fractional part

  const tradeData = {
    price: Number(data[0]),
    qty: Number(data[1]),
    time: timestamp, // the adjusted timestamp with exactly 13 digits
    isbuyermaker: (data[3] === 'b' && data[4] === 'l') || (data[3] === 's' && data[4] === 'm'),
  };
  return tradeData;
}


function encodeMessage(tradingPair, filteredData) {
    return JSON.stringify({
        header: JSON.stringify({
            exchange: "kraken",
            market: "spot",
            symbol: tradingPair,
        }),
        payload: JSON.stringify(filteredData)
    });
}
