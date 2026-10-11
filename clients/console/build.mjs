// Bundles the console into dist/: index.html, app.js, style.css. The API serves dist/ at /console.
import { build } from "esbuild";
import { copyFileSync, mkdirSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

mkdirSync("dist/licenses", { recursive: true });
await build({
  entryPoints: ["src/main.tsx"],
  bundle: true,
  format: "esm",
  target: "es2022",
  jsx: "automatic",
  jsxImportSource: "preact",
  minify: true,
  sourcemap: true,
  outfile: "dist/app.js",
});
for (const f of ["index.html", "style.css"]) copyFileSync(`src/${f}`, `dist/${f}`);
// The bundle includes Preact (MIT): ship its license with it.
const preact = dirname(createRequire(import.meta.url).resolve("preact/package.json"));
copyFileSync(join(preact, "LICENSE"), "dist/licenses/preact.LICENSE");
console.log("console built in dist/");
