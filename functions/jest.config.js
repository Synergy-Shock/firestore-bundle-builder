const packageJson = require("./package.json");

module.exports = {
  displayName: packageJson.name,
  rootDir: "./",
  preset: "ts-jest",
  transform: {
    "^.+\\.tsx?$": [
      "ts-jest",
      {
        tsconfig: "<rootDir>/__tests__/tsconfig.json",
      },
    ],
  },
  testMatch: ["**/__tests__/*.test.ts"],
  testEnvironment: "node",
};
