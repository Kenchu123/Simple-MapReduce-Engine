# Simple Distributed File System

UIUC CS425, Distributed Systems: Fall 2023 Machine Programming 4

## Description

See [MP Document](./docs/MP4.CS425.FA23.pdf)

## Installation

### Prerequisites

- Go 1.20

### Build

```bash
make build
```

## Usage

### Serve

`serve` command starts the command server and waits for commands from clients.

```bash
./bin/sdfs serve [flags]

Flags:
  -c, --config string   path to config file (default ".sdfs/config.yml")
  -h, --help            help for serve
  -l, --log string      path to log file (default "logs/sdfs.log")
```

### Membership

#### Join

`join` command tells the machine to join the group.

```bash
./bin/sdfs join [flags]

Flags:
  -h, --help   help for join
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### Leave

`leave` command tells the machine to leave the group.

```bash
./bin/sdfs leave [flags]

Flags:
  -h, --help   help for leave
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### Fail

`fail` command tells the machine's process to fail.

```bash
./bin/sdfs fail [flags]

Flags:
  -h, --help   help for fail
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### List the Membership List

`list_mem` command lists the membership list.

```bash
./bin/sdfs list_mem [flags]

Flags:
  -h, --help   help for list_mem
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### List Self's ID

`list_self` command lists self's ID.

```bash
./bin/sdfs list_self [flags]

Flags:
  -h, --help   help for list_self
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### Enable/Disable Suspicion

`enable suspicion` command enables suspicion.
`disable suspicion` command disables suspicion.

```bash
./bin/sdfs enable suspicion [flags]
./bin/sdfs disable suspicion [flags]

Flags:
  -h, --help     help for suspicion
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

### Config

#### Set DropRate

`config set-droprate` command sets the drop rate.

```bash
./bin/sdfs config set-droprate [flags]

Flags:
  -d, --droprate float32   droprate
  -h, --help               help for droprate

Global Flags:
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### Set Verbose

`config set-verbose` command sets the verbose level.

```bash
./bin/sdfs config set-verbose [flags]

Flags:
  -h, --help      help for set-verbose
  -v, --verbose   enable or disable verbose

Global Flags:
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

### SDFS (Simple Distributed File System)

#### Get File

`get` command get file from SDFS.

```bash
Usage:
  sdfs get [sdfsfilename] [localfilename] [flags]

Examples:
  sdfs get sdfs_test local_test
```

#### Put File

`put` command put file to SDFS.

```bash
Usage:
  sdfs put [localfilename] [sdfsfilename] [flags]

Examples:
  sdfs put local_test sdfs_test
```

#### Delete File

`delete` command delete file from SDFS.

```bash
Usage:
  sdfs delete sdfsfilename [flags]

Examples:
  delete sdfs_test
```

#### List File

`ls` command list file from SDFS.

```bash
Usage:
  sdfs ls [sdfsfilename] [flags]

Examples:
  sdfs ls sdfs_test
```

#### Store File

`store` command store file from SDFS.

```bash
Usage:
  sdfs store [flags]

Examples:
  sdfs store
```

#### Multiread File

`multiread` command launches multiple machines to read a file from SDFS.

```bash
Usage:
  sdfs multiread [sdfsfilename] [localfilename] [flags]

Examples:
  sdfs multiread sdfs_test local_test -m "0[1-9]"

Flags:
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -h, --help                   help for multiread
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### Multiwrite File

`multiwrite` command launches multiple machines to write a file to SDFS.

```bash
Usage:
  sdfs multiwrite [localfilename] [sdfsfilename] [flags]

Examples:
  sdfs multiwrite local_test sdfs_test -m "0[1-9]"

Flags:
  -c, --config string          path to config file (default ".sdfs/config.yml")
  -h, --help                   help for multiwrite
  -m, --machine-regex string   regex for machines to join (e.g. "0[1-9]") (default ".*")
```

#### Maple (Map)

`maple` command launches a map job.

```bash
Usage:
  sdfs maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory> [params for maple_exe] [flags]

Examples:
  maple maple_wordcount_regex 5 maple_intermediate_wc_ sdfs_src- 'hello.*'

Flags:
  -c, --config string   path to config file (default ".sdfs/config.yml")
  -h, --help            help for maple

Global Flags:
  -l, --log string   path to log file (default "logs/sdfs.log")
```

#### Juice (Reduce)

`juice` command launches a reduce job.

```bash
Usage:
  sdfs juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> [params] --delete_input={0,1} [flags]

Examples:
  juice juice_wordcount_regex 5 maple_intermediate_wc_ sdfs_dest 'hello.*' --delete_input=1

Flags:
  -c, --config string      path to config file (default ".sdfs/config.yml")
  -d, --delete_input int   delete input files after juice
  -h, --help               help for juice

Global Flags:
  -l, --log string   path to log file (default "logs/sdfs.log")
```

## Development

### Prerequisites

- Docker
- docker compose

### Set Environment

```bash
# on one session
docker compose -f docker-compose.dev.yml up [-d] [--build]

# on another session
docker exec -it dev-m[1-10] /bin/bash

$ go run main.go [command] [flags]
```

## Running on VMs

### Prerequisites

- `sdfs` binary in each VM home directory
- [tmux](https://github.com/tmux/tmux) installed

### Run Serve

Use tmux to run `sdfs serve` in background on each VM.

```bash
# run all sdfs process
./vm_run_all.sh
```

### Run Commands

```bash
# ssh to one machine
./sdfs [command] [flags]
```

### Kill

```bash
# kill all sdfs process
./vm_kill_all.sh
```

## Contributor

- [Che-Kuang Chu](https://github.com/Kenchu123)
- [Jhih-Wei Lin](https://github.com/williamlin0825)
