#!/bin/bash
BUILD=$(git log --no-merges --pretty=format:"%h" -1)
TARGET_OS=linux
TARGET_ARCH=amd64
TARGET_NAME=feed
TAR_PACKAGE="$TARGET_NAME-$BUILD".tar.gz
TMP_DIR=./feedserver

CGO_ENABLED=0 GOOS="$TARGET_OS" GOARCH="$TARGET_ARCH" godep go build -o $TARGET_NAME main.go
        
if [ $? -ne 0 ]; then
    echo "Failed to build $TARGET_NAME"
    exit 1
fi
echo "Build $TARGET_NAME, OS is $TARGET_OS, Arch is $TARGET_ARCH" 

# Make dir
mkdir -p "$TMP_DIR/logs/"
mkdir -p "$TMP_DIR/bin/"
mkdir -p "$TMP_DIR/conf/"

# copy config file
cp conf/*.toml "$TMP_DIR/conf/"
# copy see log config file
cp conf/*.xml "$TMP_DIR/conf/"
# copy bin
cp $TARGET_NAME "$TMP_DIR/bin"

# tar
tar zcfvP $TAR_PACKAGE "$TMP_DIR"
echo "tar $TAR_PACKAGE success."
rm -rf $TMP_DIR
rm -rf $TARGET_NAME
