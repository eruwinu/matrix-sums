import { cpus } from 'os';
import { basename, extname } from 'path';
import { Worker, MessageChannel } from 'worker_threads';
import { promises as fsPromises } from 'fs';
const { unlink, readFile, appendFile } = fsPromises;

import * as Bacon from 'baconjs';
import * as R from 'ramda';

import {
    Matrix, toArrayBuffer, copyToSubMatrix,
    readMatrixFromFile, appendMatrixToFile, printMatrix
} from './index';

const numWorkers = R.length(cpus());
const workers = R.map(_ => new Worker(__dirname + "/worker.js"), R.range(0, numWorkers));
let workerIndex = 0;

const INPUT_FILE = process.argv[2];

const balancedSubMatrices = Bacon.fromPromise(readMatrixFromFile(INPUT_FILE))
    .flatMapError<Matrix>(_ => {
        console.log("Error opening input file:", INPUT_FILE);
        return Bacon.never();
    })
    .flatMap<Matrix>(matrix => Bacon.fromBinder(sink => {
        const [m, n] = [R.length(matrix), R.length(R.head(matrix))];
        let squareSubMatrixSize = 2;
        let pivot = { row: 0, col: 0 };
        while (squareSubMatrixSize < R.min(m, n)) {
            while (pivot.row <= m - squareSubMatrixSize) {
                while (pivot.col <= n - squareSubMatrixSize) {
                    sink(copyToSubMatrix(matrix, squareSubMatrixSize, pivot));
                    pivot.col++;
                }
                pivot.row++;
                pivot.col = 0;
            }
            squareSubMatrixSize++;
            pivot.row = 0;
        }
        sink(new Bacon.End());
        return () => { };
    }))
    .flatMapWithConcurrencyLimit(numWorkers, matrix => {
        workerIndex++;
        const arrBuf = toArrayBuffer(matrix);
        return Bacon.fromPromise<boolean>(new Promise((resolve) => {
            const subChannel = new MessageChannel();
            subChannel.port2.once("message", resolve);
            workers[workerIndex % numWorkers].postMessage({
                buffer: arrBuf,
                m: R.length(matrix),
                n: R.length(R.head(matrix)),
                port: subChannel.port1
            }, [arrBuf.buffer, subChannel.port1]);
        }))
            .flatMap<Matrix>(isBalanced => isBalanced ? matrix : Bacon.never());
    });

balancedSubMatrices
    .onValue(printMatrix);

balancedSubMatrices
    .onEnd(() => workers.forEach(worker => worker.terminate()));

const OUTPUT_FILE = process.argv[3];
const EXT = extname(OUTPUT_FILE);
const BASE = basename(OUTPUT_FILE, EXT);

interface OutFile { size: number; file: string; };

unlink(OUTPUT_FILE)
    .catch(() => { })
    .then(() => {
        balancedSubMatrices
            .groupBy(R.compose(R.toString, R.length), (sameSizeMatrices, firstMatrix) => {
                const sqMatrixSize = R.length(firstMatrix);
                const GROUP_OUTPUT_FILE = BASE + `${sqMatrixSize}x${sqMatrixSize}` + EXT;
                return sameSizeMatrices
                    .scan<Promise<void>>(Promise.resolve(), (chain, matrix) => {
                        return chain.then(() => {
                            return appendMatrixToFile(GROUP_OUTPUT_FILE, matrix);
                        });
                    })
                    .map(R.always({ size: sqMatrixSize, file: GROUP_OUTPUT_FILE }))
                    .toEventStream();
            })
            .flatMap<OutFile>(R.identity)
            .fold<OutFile[]>([], (files, file) =>
                R.none(R.eqBy(R.prop("file"), file), files) ?
                    R.append(file, files) : files)
            .onValue(files => {
                R.reduce<string, Promise<void>>((chain, file) => {
                    return chain
                        .then(() => readFile(file))
                        .then(data => appendFile(OUTPUT_FILE, data)) // to do: use fs stream API instead
                        .then(() => unlink(file));
                }, Promise.resolve(), R.map(R.prop("file"), R.sortBy(R.prop("size"), files)));
            });
    });
