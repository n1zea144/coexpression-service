package service

import (
	"context"
	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"log"
	"net"
)

var flightClient flight.Client

func arrowFlightFetch(query string) (*flight.Reader, error) {
	log.Println("Running arrowFlightFetch...")
	var err error

	// create client
	if flightClient == nil {
		opts := make([]grpc.DialOption, 0)
		opts = append(opts, grpc.WithInsecure())
		flightClient, err = flight.NewFlightClient(net.JoinHostPort("localhost", "32010"), nil, opts...)
		if err != nil {
			return nil, err
		}
	}
	log.Println("[INFO] flight.NewFlightClient was successful.")

	// authenticate
	ctx := metadata.NewOutgoingContext(context.TODO(),
		metadata.Pairs("routing-tag", "test-routing-tag", "routing-queue", "Low Cost User Queries"))

	// Once successful, the context object now contains the credentials, use it for subsequent calls.
	if ctx, err = flightClient.AuthenticateBasicToken(ctx, "", ""); err != nil {
		return nil, err
	}
	log.Println("[INFO] Authentication was successful.")

	// query
	desc := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  []byte(query),
	}

	// get the FlightInfo message to retrieve the ticket corresponding to the query result set
	info, err := flightClient.GetFlightInfo(ctx, desc)
	if err != nil {
		return nil, err
	}
	log.Println("[INFO] flightClient.GetFlightInfo was successful.")

	// retrieve the result set as a stream of Arrow record batches.
	stream, err := flightClient.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, err
	}

	log.Println("[INFO] flightClient.DoGet was successful.")
	return flight.NewRecordReader(stream)
}
