#!/bin/bash

mkdir bin
mkdir release

javac src/br/ufrj/ppgi/huffmanyarnmultithread/*.java src/br/ufrj/ppgi/huffmanyarnmultithread/encoder/*.java src/br/ufrj/ppgi/huffmanyarnmultithread/decoder/*.java src/br/ufrj/ppgi/huffmanyarnmultithread/yarn/*.java -d bin

ant
