package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RingIntBuffer struct {
	array []int      // хранилище
	size  int        // максимальный размер буфера
	start int        // индекс самого старого элемента
	count int        // количество актуальных элементов
	mutex sync.Mutex // для синхронизации доступа
}

const RingBufferSize = 5
const timeGet time.Duration = time.Second * 20

func NewRingIntBuffer() *RingIntBuffer {
	return &RingIntBuffer{
		array: make([]int, RingBufferSize),
		size:  RingBufferSize,
		start: 0,
		count: 0,
	}
}

func (r *RingIntBuffer) Push(el int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	end := (r.start + r.count) % r.size
	r.array[end] = el

	if r.count == r.size {
		r.start = (r.start + 1) % r.size
	} else {
		r.count++
	}
}

func (r *RingIntBuffer) Get() []int {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.count == 0 {
		return nil
	}
	output := make([]int, r.count)
	for i := 0; i < r.count; i++ {
		output[i] = r.array[(r.start+i)%r.size]
	}

	return output
}

type stageInt func(<-chan bool, <-chan int) <-chan int

type Pipeline struct {
	stages []stageInt
	done   <-chan bool
}

func NewPipeline(done <-chan bool, stages ...stageInt) *Pipeline {
	return &Pipeline{
		stages: stages,
		done:   done,
	}
}

func (p *Pipeline) Run(source <-chan int) <-chan int {
	var c <-chan int = source
	for stage := 0; stage < len(p.stages); stage++ {
		c = p.RunStageInt(p.stages[stage], c)
	}
	return c
}

func (p *Pipeline) RunStageInt(stage stageInt, source <-chan int) <-chan int {
	return stage(p.done, source)
}

func main() {

	dataSource := func() (<-chan int, <-chan bool) {
		scanner := bufio.NewScanner(os.Stdin)

		source := make(chan int)
		done := make(chan bool)
		go func() {
			defer close(done)
			defer close(source)
			for scanner.Scan() {
				text := scanner.Text()
				if strings.EqualFold(text, "exit") {
					done <- true
					return
				}
				val, err := strconv.Atoi(text)

				if err != nil {
					fmt.Println("Принимаются только целые числа")
					continue
				}
				source <- val
			}
		}()
		return source, done
	}

	positiveFilter := func(done <-chan bool, input <-chan int) <-chan int {
		positiveNums := make(chan int)
		go func() {
			defer close(positiveNums)
			for {
				select {
				case <-done:
					return
				case val, isOpen := <-input:
					if !isOpen {
						return
					}
					if val > 0 {
						select {
						case <-done:
							return
						case positiveNums <- val:
						}
					}
				}
			}
		}()
		return positiveNums
	}

	threeFilter := func(done <-chan bool, input <-chan int) <-chan int {
		threeNums := make(chan int)

		go func() {
			defer close(threeNums)
			for {
				select {
				case <-done:
					return
				case val, isOpen := <-input:
					if !isOpen {
						return
					}
					if val%3 == 0 && val != 0 {
						select {
						case <-done:
							return
						case threeNums <- val:
						}
					}
				}
			}
		}()
		return threeNums
	}

	bufferStageInt := func(done <-chan bool, input <-chan int) <-chan int {
		bufferedNumbers := make(chan int)
		buffer := NewRingIntBuffer()
		go func() {
			defer close(bufferedNumbers)

			for {
				select {
				case <-done:
					return
				case val := <-input:
					buffer.Push(val)
				}
			}
		}()
		go func() {
			for {
				select {
				case <-time.After(timeGet):
					bufferValues := buffer.Get()
					if bufferValues != nil {
						for _, val := range bufferValues {
							select {
							case <-done:
								return
							case bufferedNumbers <- val:
							}
						}
					}
				case <-done:
					return
				}
			}
		}()
		return bufferedNumbers
	}

	consumer := func(done <-chan bool, source <-chan int) {
		for {
			select {
			case <-done:
				return
			case val := <-source:
				fmt.Printf("Обработанны числа %v\n", val)
			}
		}
	}

	source, done := dataSource()

	pipeline := NewPipeline(done, positiveFilter, threeFilter, bufferStageInt)
	consumer(done, pipeline.Run(source))
}
