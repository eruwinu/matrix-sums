import * as Bacon from 'baconjs';
import * as R from 'ramda';
import { log } from 'console';
import { Worker, isMainThread, parentPort, MessageChannel } from 'worker_threads';
import { cpus } from 'os';
import { readFile, appendFileSync, unlinkSync } from 'fs';

type Matrix = number[][];
type MatrixArrayBuffer = Int8Array;

function toArrayBuffer(matrix: Matrix): MatrixArrayBuffer {
    const [m, n] = [R.length(matrix), R.length(R.head(matrix))];
    let arrBuf = new Int8Array(m*n).fill(0);
    for (let row of R.range(0, m)) {
        for (let col of R.range(0, n)) {
            arrBuf[row*m + col] = matrix[row][col];
        }
    }
    return arrBuf;
}
function toMatrix(arrBuf: MatrixArrayBuffer, m: number, n: number): Matrix {
    let matrix = generateMatrix(m, n);
    for (let row of R.range(0, m)) {
        for (let col of R.range(0, n)) {
            matrix[row][col] = arrBuf[row*m + col];
        }
    }
    return matrix;
}

function generateMatrix(m: number, n: number) {
    const rows = R.range(0, m);
    const matrix = R.map(_ => R.range(0, n), rows);
    return matrix;
}

function copyToSubMatrix(matrix: Matrix, squareSubMatrixSize: number, pivot: {row: number, col: number}) {
    return R.reduce((submatrix, [row, col]) => {
            submatrix[row][col] = matrix[pivot.row + row][pivot.col + col];
            return submatrix;
        },
        generateMatrix(squareSubMatrixSize, squareSubMatrixSize),
        R.xprod(R.range(0, squareSubMatrixSize), R.range(0, squareSubMatrixSize)));
}

function formatMatrix(matrix: Matrix) {
    return R.join('\n', R.map(R.join(' '), matrix));
}

function printMatrix(matrix: Matrix) {
    log(formatMatrix(matrix) + '\n');
}
function appendMatrixToFileSync(filePath: string, matrix: Matrix) {
    appendFileSync(filePath, formatMatrix(matrix) + '\n\n');
}

function readMatrixFromFile(filePath: string) {
    return new Promise<string>((resolve, reject) => {
            readFile(filePath, 'utf8', (err, data) => {
                if (err) reject(err);
                else resolve(data);
            });
        })
        .then(data => {
            return data.split('\n')
                .map(line => line.split(' ').map(n => parseInt(n)));
        });
}
function getDiagonals(matrix: Matrix) {
    const [m, n] = [R.length(matrix), R.length(R.head(matrix))];
    const mapper = ([row, col]: [number, number]) => {
        return matrix[row][col];
    };
    const backwardSlashDiagonal = R.map(mapper, R.zip(R.range(0, m), R.range(0, n)));
    const forwardSlashDiagonal = R.map(mapper, R.zip(R.reverse(R.range(0, m)), R.range(0, n)));
    return [forwardSlashDiagonal, backwardSlashDiagonal];
}


if (isMainThread) {
    const numWorkers = R.length(cpus());
    const workers = R.map(_ => new Worker(__filename), R.range(0, numWorkers));
    let workerIndex = 0;

    const balancedSubMatrices = Bacon.fromPromise(readMatrixFromFile(process.argv[2]))
        .flatMap(matrix => Bacon.fromBinder<unknown, Matrix>(sink => {
            const [m, n] = [R.length(matrix), R.length(R.head(matrix))];
            let squareSubMatrixSize = 2;
            while (squareSubMatrixSize < R.min(m, n)) {
                let pivot = {row: 0, col: 0};
                while (pivot.row <= m - squareSubMatrixSize) {
                    while (pivot.col <= n - squareSubMatrixSize) {
                        sink(copyToSubMatrix(matrix, squareSubMatrixSize, pivot));
                        pivot.col++;
                    }
                    pivot.row++;
                    pivot.col = 0;
                }
                squareSubMatrixSize++;
            }
            sink(new Bacon.End());
            return () => {};
        }))
        .flatMapWithConcurrencyLimit(numWorkers, matrix => {
            workerIndex++;
            const arrBuf = toArrayBuffer(matrix);
            return Bacon.fromPromise(new Promise<boolean>((resolve) => {
                    const subChannel = new MessageChannel();
                    subChannel.port2.once("message", resolve);
                    workers[workerIndex % numWorkers].postMessage({
                        buffer: arrBuf,
                        m: R.length(matrix),
                        n: R.length(R.head(matrix)),
                        port: subChannel.port1
                    }, [arrBuf.buffer, subChannel.port1]);
                }))
                .flatMap<Matrix>(isBalanced => isBalanced ? Bacon.once(matrix) : Bacon.never());
        });

        balancedSubMatrices
            .onValue(printMatrix);
        unlinkSync(process.argv[3]);
        balancedSubMatrices
            .onValue(matrix => {
                appendMatrixToFileSync(process.argv[3], matrix);
            })
        balancedSubMatrices
            .onEnd(() => workers.forEach(worker => worker.terminate()));

} else if (parentPort) {
    parentPort.on("message", value => {
        const matrix = toMatrix(value.buffer, value.m, value.n);
        const getRows = R.identity;
        const getColumns = R.transpose;
        Bacon.fromArray(getRows(matrix))
            .merge(Bacon.fromArray(getColumns(matrix)))
            .merge(Bacon.fromArray(getDiagonals(matrix)))
            .map(R.sum)
            .slidingWindow(2, 2)
            .scan({allEqual: true, continue: true}, (memo, [sum1, sum2]) => ({
                continue: memo.allEqual,
                allEqual: memo.allEqual && sum1 === sum2
            }))
            .takeWhile(R.prop("continue"))
            .map(R.prop("allEqual"))
            .last()
            .onValue(isBalanced => {
                value.port.postMessage(isBalanced);
                value.port.close();
            });
    });
}