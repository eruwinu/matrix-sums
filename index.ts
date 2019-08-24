import { promises as fsPromises } from 'fs';
const { readFile, appendFile } = fsPromises;

import * as R from 'ramda';

export type Matrix = number[][];
export type MatrixArrayBuffer = Int8Array;

export function generateMatrix(m: number, n: number) {
    const rows = R.range(0, m);
    const matrix = R.map(_ => R.range(0, n), rows);
    return matrix;
}

export function toArrayBuffer(matrix: Matrix): MatrixArrayBuffer {
    const [m, n] = [R.length(matrix), R.length(R.head(matrix))];
    let arrBuf = new Int8Array(m * n).fill(0);
    for (let row of R.range(0, m)) {
        for (let col of R.range(0, n)) {
            arrBuf[row * m + col] = matrix[row][col];
        }
    }
    return arrBuf;
}

export function toMatrix(arrBuf: MatrixArrayBuffer, m: number, n: number): Matrix {
    let matrix = generateMatrix(m, n);
    for (let row of R.range(0, m)) {
        for (let col of R.range(0, n)) {
            matrix[row][col] = arrBuf[row * m + col];
        }
    }
    return matrix;
}

export function copyToSubMatrix(matrix: Matrix, squareSubMatrixSize: number, pivot: { row: number, col: number }) {
    return R.reduce((submatrix, [row, col]) => {
        submatrix[row][col] = matrix[pivot.row + row][pivot.col + col];
        return submatrix;
    },
        generateMatrix(squareSubMatrixSize, squareSubMatrixSize),
        R.xprod(R.range(0, squareSubMatrixSize), R.range(0, squareSubMatrixSize)));
}

const CRLF = "\r\n";
const NEWLINE = CRLF;

export function formatMatrix(matrix: Matrix) {
    return R.join(NEWLINE, R.map(R.join(' '), matrix));
}

export function printMatrix(matrix: Matrix) {
    console.log(formatMatrix(matrix) + '\n');
}

export function appendMatrixToFile(filePath: string, matrix: Matrix) {
    return appendFile(filePath, formatMatrix(matrix) + NEWLINE + NEWLINE);
}

export function readMatrixFromFile(filePath: string) {
    return readFile(filePath, 'utf8')
        .then(data => {
            return data.trim().split(/\r?\n/)
                .map(line => line.trim().split(' ').map(n => parseInt(n)));
        });
}
