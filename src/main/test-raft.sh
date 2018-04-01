#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
PROJECT="$here/../../"
export GOPATH="$PROJECT"

for name in $(sed -nE "s/^func Test([a-zA-Z0-9_]+).*/\1/p" $PROJECT/src/raft/test_test.go); do
    FAIL=0
    N=0
    for i in {1..10}; do
        let "N+=1"
        echo > /tmp/$name.$i.log
        go test -v -race -run $name -timeout 30s raft/... 2>>/tmp/$name.$i.log >>/tmp/$name.$i.log &
    done

    for job in `jobs -p`; do
        wait $job || let "FAIL+=1"
    done

    echo "==> $name executes($N) failed($FAIL)"
    if [ $FAIL -eq $N ]; then
        exit 
    fi
done

