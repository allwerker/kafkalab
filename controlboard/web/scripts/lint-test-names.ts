/**
 * Fails when a test does not follow the project convention (CLAUDE.md):
 *  - every it()/test() name matches /^should[A-Z].*When[A-Z]/
 *  - every test body contains // GIVEN:, // WHEN: and // THEN: comments
 * Simple regex scan over src/**\/*.test.ts(x); good enough for our test style.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const NAME = /^should[A-Z].*When[A-Z]/;
const root = join(import.meta.dir, "..");

function files(dir: string): string[] {
  return readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    if (statSync(p).isDirectory()) return f === "node_modules" ? [] : files(p);
    return /\.test\.tsx?$/.test(f) ? [p] : [];
  });
}

const problems: string[] = [];
let count = 0;
for (const file of files(join(root, "src"))) {
  const src = readFileSync(file, "utf8");
  const re = /\b(?:it|test)(?:\.(?:only|skip|each\([^)]*\)))?\(\s*(["'`])((?:\\.|(?!\1).)*)\1/g;
  const starts: { name: string; index: number; line: number }[] = [];
  for (let m = re.exec(src); m; m = re.exec(src)) starts.push({ name: m[2]!, index: m.index, line: src.slice(0, m.index).split("\n").length });
  starts.forEach((t, i) => {
    count++;
    const where = `${relative(root, file)}:${t.line}`;
    if (!NAME.test(t.name)) problems.push(`${where}  name "${t.name}" does not match ${NAME}`);
    const body = src.slice(t.index, starts[i + 1]?.index ?? src.length);
    for (const tag of ["GIVEN", "WHEN", "THEN"]) if (!new RegExp(`//\\s*${tag}:`).test(body)) problems.push(`${where}  "${t.name}" lacks a // ${tag}: comment`);
  });
}

if (problems.length) {
  console.error(`Test convention violations (${problems.length}):\n${problems.join("\n")}`);
  process.exit(1);
}
console.log(`test names OK (${count} tests follow should…When… with GIVEN/WHEN/THEN)`);
