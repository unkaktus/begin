package main

import (
	"flag"
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

type Config struct {
	Name                         string
	NumberOfNodes                int
	NodeType                     string
	NumberOfMPIRanksPerNode      int
	NumberOfOMPThreadsPerProcess int
	Walltime                     time.Duration
	Email                        string
	LogDirectory                 string

	LoadModules []string

	WorkingDirectory string
	EntryPoint       string
}

func (config Config) PBS() string {
	builder := &strings.Builder{}
	NumberOfMPIRanks := config.NumberOfNodes * config.NumberOfMPIRanksPerNode

	OutputFile := path.Join(config.LogDirectory, config.Name+".out")
	ErrorFile := path.Join(config.LogDirectory, config.Name+".err")

	builder.WriteString(`#!/bin/bash -l
#PBS -N ` + config.Name + `
#PBS -e ` + ErrorFile + `
#PBS -o ` + OutputFile + `
#PBS -m abe
#PBS -M ` + config.Email + `
#PBS -l select=` + strconv.Itoa(config.NumberOfNodes) +
		`:node_type=` + config.NodeType +
		`:mpiprocs=` + strconv.Itoa(NumberOfMPIRanks) +
		`:ompthreads=` + strconv.Itoa(config.NumberOfOMPThreadsPerProcess) + `
#PBS -l walltime=` + formatDuration(config.Walltime) + `
`)
	for _, module := range config.LoadModules {
		builder.WriteString(fmt.Sprintf("module load %s\n", module))
	}

	builder.WriteString("time mpirun" +
		" -x OMP_DISPLAY_ENV=TRUE" +
		" -x OMP_NUM_THREADS=" + strconv.Itoa(config.NumberOfOMPThreadsPerProcess) +
		" -x OMP_PLACES=cores" +
		" -n " + strconv.Itoa(NumberOfMPIRanks) +
		" --map-by l3cache" +
		" --bind-to l3cache" +
		" " + config.EntryPoint)

	return builder.String()
}

func run() error {
	batchSystem := flag.String("b", "", "Batch system to use [pbs slurm]")
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatal("Job file is not specified")
	}
	filename := flag.Args()[0]

	config := Config{}

	_, err := toml.DecodeFile(filename, &config)
	if err != nil {
		return fmt.Errorf("decode file: %w", err)
	}

	switch *batchSystem {
	case "pbs":
		fmt.Printf("%s\n", config.PBS())
	case "slurm":
		return fmt.Errorf("not implemented yet")
	default:
		return fmt.Errorf("batch system is not specified")
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
