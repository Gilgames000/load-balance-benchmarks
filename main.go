package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"text/tabwriter"
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
			c.WaitingTime = time.Now().Sub(c.ArrivalTime)
			t := rand.ExpFloat64() / mu
			serviceTime := float64ToDuration(t)
			<-time.After(serviceTime)
			c.ServiceTime = serviceTime
			out <- c
		}
	}
}

func randomFanOut(customers <-chan Customer, servers []chan Customer, d int, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case c := <-customers:
			s := rand.Perm(len(servers))[:d]
			best := s[0]
			for _, i := range s[1:] {
				if len(servers[best]) > len(servers[i]) {
					best = i
				}
			}
			servers[best] <- c
		}
	}
}

func shortestQueueSubsetBalancer(lambda, mu float64, n, d int, t time.Duration, m int) []Customer {
	var result []Customer
	done := make(chan struct{})
	customers := make(chan Customer, 8192)
	servers := make([]chan Customer, n)
	out := make(chan Customer, 2048)

	for i := 0; i < n; i++ {
		servers[i] = make(chan Customer, 2048)
		go serveCustomers(mu, servers[i], out, done)
	}
	go randomFanOut(customers, servers, d, done)
	go generateCustomers(lambda, customers, done)

	go func() {
		select {
		case <-time.After(t):
			close(done)
		case <-done:
			return
		}
	}()

	for {
		select {
		case <-done:
			return result
		case c := <-out:
			result = append(result, c)
			if len(result) == m {
				return result
			}
		}
	}
}

func shortestQueueBalancer(lambda, mu float64, n int, t time.Duration, m int) []Customer {
	return shortestQueueSubsetBalancer(lambda, mu, n, n, t, m)
}

func randomBalancer(lambda, mu float64, n int, t time.Duration, m int) []Customer {
	return shortestQueueSubsetBalancer(lambda, mu, n, 1, t, m)
}

func singleBigServer(lambda, mu float64, t time.Duration, m int) []Customer {
	return randomBalancer(lambda, mu, 1, t, m)
}

func averageWaitingTime(customers []Customer) time.Duration {
	var sum float64

	for _, c := range customers {
		sum += c.WaitingTime.Seconds()
	}

	return float64ToDuration(sum / float64(len(customers)))
}

func averageResponseTime(customers []Customer) time.Duration {
	var sum float64

	for _, c := range customers {
		sum += c.WaitingTime.Seconds()
		sum += c.ServiceTime.Seconds()
	}

	return float64ToDuration(sum / float64(len(customers)))
}

func averageServiceTime(customers []Customer) time.Duration {
	var sum float64

	for _, c := range customers {
		sum += c.ServiceTime.Seconds()
	}

	return float64ToDuration(sum / float64(len(customers)))
}

func totalTime(customers []Customer) time.Duration {
	firstArrival := customers[0].ArrivalTime
	lastCustomer := customers[len(customers)-1]
	lastService := lastCustomer.ArrivalTime.Add(lastCustomer.WaitingTime)

	return lastService.Sub(firstArrival)
}

func runBenchmarks(lambda, mu float64, serverCount int, timeLimit time.Duration, customersLimit int) {
	random := randomBalancer(lambda*float64(serverCount), mu, serverCount, timeLimit, customersLimit)
	single := singleBigServer(lambda*float64(serverCount), mu*float64(serverCount), timeLimit, customersLimit)
	sq := shortestQueueBalancer(lambda*float64(serverCount), mu, serverCount, timeLimit, customersLimit)
	sq2 := shortestQueueSubsetBalancer(lambda*float64(serverCount), mu, serverCount, 2, timeLimit, customersLimit)

	const padding = 3
	w := tabwriter.NewWriter(os.Stdout, 0, 0, padding, ' ', 0)
	fmt.Fprintf(w, "BALANCER\tAVG_WAIT\tAVG_SERV\tAVG_RESP\tTIME\tCUSTOMERS\t\n")
	fmt.Fprintf(w, "Rand\t%v\t%v\t%v\t%v\t%v\t\n", averageWaitingTime(random), averageServiceTime(random), averageResponseTime(random), totalTime(random), len(random))
	fmt.Fprintf(w, "Single\t%v\t%v\t%v\t%v\t%v\t\n", averageWaitingTime(single), averageServiceTime(single), averageResponseTime(single), totalTime(single), len(single))
	fmt.Fprintf(w, "SQ\t%v\t%v\t%v\t%v\t%v\t\n", averageWaitingTime(sq), averageServiceTime(sq), averageResponseTime(sq), totalTime(sq), len(sq))
	fmt.Fprintf(w, "SQ(2)\t%v\t%v\t%v\t%v\t%v\t\n", averageWaitingTime(sq2), averageServiceTime(sq2), averageResponseTime(sq2), totalTime(sq2), len(sq2))
	fmt.Fprintf(w, "(λ=%.3f, μ=%.3f, serverCount=%d, timeLimit=%v, customersLimit=%d)\n\n", lambda, mu, serverCount, timeLimit, customersLimit)
	w.Flush()
}

func main() {
	runBenchmarks(0.75, 1.0, 10, 60*time.Second, 0xFEE1DEAD)
	runBenchmarks(0.75, 1.0, 10, 1337*time.Hour, 500)
}
