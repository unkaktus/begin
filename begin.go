package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	BatchPBS        = "pbs"
	BatchSlurm      = "slurm"
	BatchBare       = "bare"
	BatchAutodetect = "autodetect"
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
	PrintOMPEnvironment          bool

	LoadModules []string

	WorkingDirectory string

	PreScript []string

	RunTime    []string
	Executable string
	Arguments  []string

	PostScript []string
}

type PBSConfig struct {
	Config
	NumberOfMPIRanks int
	WalltimeString   string
	OutputFile       string
	ErrorFile        string
}

func NewPBSConfig(c Config) PBSConfig {
	cc := PBSConfig{
		Config:         c,
		WalltimeString: formatDuration(c.Walltime),
		OutputFile:     path.Join(c.LogDirectory, c.Name+".out"),
		ErrorFile:      path.Join(c.LogDirectory, c.Name+".err"),
	}
	return cc
}

type MPIRunConfig struct {
	Config
	NumberOfMPIRanks int
}

func NewMPIRunConfig(c Config) MPIRunConfig {
	cc := MPIRunConfig{
		Config:           c,
		NumberOfMPIRanks: c.NumberOfNodes * c.NumberOfMPIRanksPerNode,
	}
	return cc
}

func ExecTemplate(ts string, s interface{}) (string, error) {
	t, err := template.New("template").Parse(ts)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)

	}
	builder := &strings.Builder{}

	err = t.Execute(builder, s)
	if err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return builder.String(), nil
}

func (config Config) PBS() (string, error) {
	builder := &strings.Builder{}

	pbsString, err := ExecTemplate(`#!/bin/bash -l
#PBS -N {{.Name}}
#PBS -e {{.ErrorFile}}
#PBS -o {{.OutputFile}}
#PBS -m abe
#PBS -M {{.Email}}
#PBS -l select={{.NumberOfNodes}}`+
		`:node_type={{.NodeType}}`+
		`:mpiprocs={{.NumberOfMPIRanksPerNode}}`+
		`:ompthreads={{.NumberOfOMPThreadsPerProcess}}`+`
#PBS -l walltime={{.WalltimeString}}
`,
		NewPBSConfig(config),
	)
	if err != nil {
		return "", fmt.Errorf("create mpirun string: %w", err)
	}

	builder.WriteString(pbsString)

	builder.WriteString("\n")

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

	mpirunString, err := ExecTemplate("time mpirun"+
		" -x OMP_NUM_THREADS={{.NumberOfOMPThreadsPerProcess}}"+
		" -x OMP_PLACES=cores"+
		" -n {{.NumberOfMPIRanks}}"+
		" --map-by node:PE={{.NumberOfOMPThreadsPerProcess}}"+
		" --bind-to core",
		NewMPIRunConfig(config),
	)
	if err != nil {
		return "", fmt.Errorf("create mpirun string: %w", err)
	}

	task := []string{}
	if mpirunString != "" {
		task = append(task, mpirunString)
	}
	if len(config.RunTime) > 0 {
		task = append(task, strings.Join(config.RunTime, " "))
	}

	task = append(task, config.Executable)

	if len(config.Arguments) > 0 {
		task = append(task, strings.Join(config.Arguments, " "))
	}

	builder.WriteString(strings.Join(task, " "))
	builder.WriteString("\n")

	for _, cmd := range config.PostScript {
		builder.WriteString(cmd + "\n")
	}

	return builder.String(), nil
}

func run() error {
	batchSystem := flag.String("b", BatchAutodetect, "Batch system to use [pbs, slurm], or default to autodetect")
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

	if *batchSystem == BatchAutodetect {
		*batchSystem = DetectBatchSystem()
	}

	switch *batchSystem {
	case BatchPBS:
		jobData, err := config.PBS()
		if err != nil {
			return fmt.Errorf("getting PBS job data: %w", err)
		}
		fmt.Printf("%s\n", jobData)
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
