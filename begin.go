package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	BatchPBS   = "pbs"
	BatchSlurm = "slurm"
	BatchBare  = "bare"
)

func DetectBatchSystem() string {
	if _, err := exec.LookPath("qsub"); err == nil {
		return BatchPBS
	}
	if _, err := exec.LookPath("squeue"); err == nil {
		return BatchSlurm
	}
	return BatchBare
}

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

	PreScript  []string
	EntryPoint string
	PostScript []string
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
		`:mpiprocs=` + strconv.Itoa(config.NumberOfMPIRanksPerNode) +
		`:ompthreads=` + strconv.Itoa(config.NumberOfOMPThreadsPerProcess) + `
#PBS -l walltime=` + formatDuration(config.Walltime) + `

`)
	for _, module := range config.LoadModules {
		builder.WriteString(fmt.Sprintf("module load %s\n", module))
	}
	builder.WriteString("\n")

	for _, cmd := range config.PreScript {
		builder.WriteString(cmd + "\n")
	}
	builder.WriteString("\n")

	if config.WorkingDirectory != "" {
		builder.WriteString("cd " + config.WorkingDirectory + "\n")
	}
	builder.WriteString("\n")

	builder.WriteString("time mpirun" +
		" -x OMP_DISPLAY_ENV=TRUE" +
		" -x OMP_NUM_THREADS=" + strconv.Itoa(config.NumberOfOMPThreadsPerProcess) +
		" -x OMP_PLACES=cores" +
		" -n " + strconv.Itoa(NumberOfMPIRanks) +
		" --map-by node:PE=" + strconv.Itoa(config.NumberOfOMPThreadsPerProcess) +
		" --bind-to core" +
		" " + config.EntryPoint + "\n")
	builder.WriteString("\n")

	for _, cmd := range config.PostScript {
		builder.WriteString(cmd + "\n")
	}

	return builder.String()
}

func run() error {
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

	switch DetectBatchSystem() {
	case BatchPBS:
		fmt.Printf("%s\n", config.PBS())
	default:
		return fmt.Errorf("batch system is not supported")
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
