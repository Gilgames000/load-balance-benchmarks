package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type Customer struct {
	ArrivalTime time.Time
	WaitingTime time.Duration
	ServiceTime time.Duration
}

func float64ToDuration(seconds float64) time.Duration {
	seconds, milliseconds := math.Modf(seconds)
	milliseconds, microseconds := math.Modf(milliseconds * 1000)
	microseconds, nanoseconds := math.Modf(microseconds * 1000)
	nanoseconds, _ = math.Modf(nanoseconds * 1000)
	t := time.Duration(seconds) * time.Second
	t += time.Duration(milliseconds) * time.Millisecond
	t += time.Duration(microseconds) * time.Microsecond
	t += time.Duration(nanoseconds) * time.Nanosecond

	return t
}

func generateCustomers(lambda float64, customers chan<- Customer, done <-chan struct{}) {
	for {
		t := rand.ExpFloat64() / lambda
		waitingTime := float64ToDuration(t)
		select {
		case <-done:
			return
		case <-time.After(waitingTime):
			customers <- Customer{ArrivalTime: time.Now()}
		}
	}
}

func serveCustomers(mu float64, customers <-chan Customer, out chan<- Customer, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case c := <-customers:
			t := rand.ExpFloat64() / mu
			serviceTime := float64ToDuration(t)
			c.WaitingTime = time.Now().Sub(c.ArrivalTime)
			<-time.After(serviceTime)
			c.ServiceTime = serviceTime
			out <- c
		}
	}
}

func randomFanOut(customers <-chan Customer, servers []chan Customer, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case c := <-customers:
			servers[rand.Intn(len(servers))] <- c
		}
	}
}

func randomBalancer(lambda, mu float64, n int, t time.Duration) []Customer {
	var result []Customer
	done := make(chan struct{})
	customers := make(chan Customer, 2048)
	servers := make([]chan Customer, n)
	out := make(chan Customer, 2048)

	for i := 0; i < n; i++ {
		servers[i] = make(chan Customer, 2048)
		go serveCustomers(mu, servers[i], out, done)
	}
	go randomFanOut(customers, servers, done)
	go generateCustomers(lambda, customers, done)

	time.AfterFunc(t, func() {
		close(done)
	})

	for {
		select {
		case <-done:
			return result
		case c := <-out:
			result = append(result, c)
		}
	}
}

func averageWaitingTime(customers []Customer) time.Duration {
	var sum float64

	for _, c := range customers {
		sum += c.WaitingTime.Seconds()
	}

	return float64ToDuration(sum / float64(len(customers)))
}

func main() {
	t := 10 * time.Second
	n := 5
	lambda := 75.0
	mu := 100.0

	res := randomBalancer(lambda, mu, n, t)
	fmt.Println(res)
	fmt.Println(len(res))
	fmt.Printf("Average waiting time: %v", averageWaitingTime(res))
}
