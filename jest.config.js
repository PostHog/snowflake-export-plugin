/** @type {import('ts-jest/dist/types').InitialOptionsTsJest} */
module.exports = {
    transform: {
        '^.+\\.(t|j)s$': ['@swc/jest'],
    },
    testEnvironment: 'node',

    collectCoverage: true,
    coverageDirectory: 'coverage',
}
