import { parentPort, MessagePort } from 'worker_threads';

import * as Bacon from 'baconjs';
import * as R from 'ramda';

import { Matrix, MatrixArrayBuffer, toMatrix } from './index';

interface MessageValue {
    buffer: MatrixArrayBuffer;
    m: number;
    n: number;
    port: MessagePort;
};

function getDiagonals(matrix: Matrix) {
    const [m, n] = [R.length(matrix), R.length(R.head(matrix))];
    const mapper = ([row, col]: [number, number]) => {
        return matrix[row][col];
    };
    const backwardSlashDiagonal = R.map(mapper, R.zip(R.range(0, m), R.range(0, n)));
    const forwardSlashDiagonal = R.map(mapper, R.zip(R.reverse(R.range(0, m)), R.range(0, n)));
    return [forwardSlashDiagonal, backwardSlashDiagonal];
}

if (parentPort) {
    parentPort.on("message", (value: MessageValue) => {
        const matrix = toMatrix(value.buffer, value.m, value.n);
        const getRows = R.identity;
        const getColumns = R.transpose;
        Bacon.fromArray(getRows(matrix))
            .merge(Bacon.fromArray(getColumns(matrix)))
            .merge(Bacon.fromArray(getDiagonals(matrix)))
            .map(R.sum)
            .slidingWindow(2, 2)
            .scan({ allEqual: true, continue: true }, (memo, [sum1, sum2]) => ({
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