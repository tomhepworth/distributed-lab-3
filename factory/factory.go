package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"pairbroker/stubs"
)

var MultiplyHandler = "Factory.Multiply"
var DivideHandler = "Factory.Divide"
var pipe = make(chan int)

type Factory struct{}

//TODO: Define a Multiply function to be accessed via RPC.
//Check the previous weeks' examples to figure out how to do this.

func (f *Factory) Multiply(req stubs.Pair, res *stubs.JobReport) (err error) {
	result := req.X * req.Y
	fmt.Println("Multiplied", req.X, " ", req.Y, " and got ", result)
	res.Result = result

	pipe <- result

	return err
}

func (f *Factory) Divide(req stubs.Pair, res *stubs.JobReport) (err error) {
	result := req.X / req.Y
	fmt.Println("Divided", req.X, " ", req.Y, " and got ", result)
	res.Result = result
	return err
}

func pipeline(client *rpc.Client) {
	for {
		x := <-pipe
		y := <-pipe
		newpair := stubs.Pair{x, y}
		status := new(stubs.StatusReport)
		towork := stubs.PublishRequest{Topic: "divide", Pair: newpair}
		err := client.Call(stubs.Publish, towork, status)
		if err != nil {
			fmt.Println("Cry")
		}
	}
}

func main() {
	pAddr := flag.String("ip", "127.0.0.1:8050", "IP and port to listen on")
	brokerAddr := flag.String("broker", "127.0.0.1:8030", "Address of broker instance")
	flag.Parse()
	client, _ := rpc.Dial("tcp", *brokerAddr)
	status := new(stubs.StatusReport)
	defer client.Close()

	client.Call(stubs.CreateChannel, stubs.ChannelRequest{Topic: "divide", Buffer: 10}, status)

	//Mult
	subs := stubs.Subscription{Topic: "multiply", FactoryAddress: *pAddr, Callback: MultiplyHandler}
	client.Go(stubs.Subscribe, subs, status, nil)

	//Div
	subs2 := stubs.Subscription{Topic: "divide", FactoryAddress: *pAddr, Callback: DivideHandler}
	client.Go(stubs.Subscribe, subs2, status, nil)

	go pipeline(client)

	rpc.Register(&Factory{})
	listener, _ := net.Listen("tcp", *pAddr)
	defer listener.Close()
	rpc.Accept(listener)
}
