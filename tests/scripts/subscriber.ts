

const url = "http://localhost:50510/ws"

const ws = new WebSocket(url)

ws.onopen = () => {
  console.log("Connected to ws server")
  ws.send(JSON.stringify({
    Subscribe: {
      table: "*",
      objs: ["SH600000"]
    }
  }))
}

ws.onmessage = (event) => {
  console.log(event.data)
}


process.on("SIGINT", () => {
  console.log("Ctrl-C was pressed");
  ws.close();
  process.exit();
});