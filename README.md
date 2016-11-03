Data-sync
===

Install
---
Golang must be installed with config `$GOROOT` & `$GOPATH`

Pull library with command

```
go get
```

Usage
---
Build project

```
go build
```

Execute binary file with args

```
./data-sync <action> <config_path>
```

- *action*: must be `import` or `export`
- *config_path*: path to table json file, file name with be known as `table name`. Example: `config/tables/user.json` - table name = `user`

Config
---
Must be in same path with binary file, in folder

```
config/config.yaml
```

Update connection configuration in `config.yaml`