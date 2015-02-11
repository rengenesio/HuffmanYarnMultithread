#!/bin/bash

mkdir bin
mkdir release

javac src/br/ufrj/ppgi/huffmanyarn/*.java src/br/ufrj/ppgi/huffmanyarn/encoder/*.java src/br/ufrj/ppgi/huffmanyarn/decoder/*.java src/br/ufrj/ppgi/huffmanyarn/yarn/*.java -d bin

ant
