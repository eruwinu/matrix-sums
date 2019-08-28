import { parentPort, MessagePort } from 'worker_threads';

import * as R from 'ramda';

import {
    Matrix, toArrayBuffer, copyToSubMatrix,
    readMatrixFromFile
} from './index';

interface MessageValue {
    inputFile: string;
    port: MessagePort;
};

if (parentPort) {
    parentPort.on("message", (value: MessageValue) => {
        readMatrixFromFile(value.inputFile)
        .then(matrix => {
            const [m, n] = [R.length(matrix), R.length(R.head(matrix))];
            let squareSubMatrixSize = 2;
            let pivot = { row: 0, col: 0 };
            while (squareSubMatrixSize < R.min(m, n)) {
                while (pivot.row <= m - squareSubMatrixSize) {
                    while (pivot.col <= n - squareSubMatrixSize) {
                        const subMatrix = copyToSubMatrix(matrix, squareSubMatrixSize, pivot);
                        const arrBuf = toArrayBuffer(subMatrix);
                        value.port.postMessage({
                            buffer: arrBuf,
                            m: squareSubMatrixSize,
                            n: squareSubMatrixSize
                        }, [arrBuf.buffer]);
                        pivot.col++;
                    }
                    pivot.row++;
                    pivot.col = 0;
                }
                squareSubMatrixSize++;
                pivot.row = 0;
            }
            value.port.close();
        })
        .catch(err => {
            console.error(err);
            value.port.close()
        });
    });
}

