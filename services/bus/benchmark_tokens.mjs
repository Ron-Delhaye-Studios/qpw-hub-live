// Encodings are bundled in this pinned dev dependency. No network/model call.
import { getEncoding } from 'js-tiktoken';
import fs from 'node:fs';
const texts=JSON.parse(fs.readFileSync(0,'utf8'));
const encoding=getEncoding('cl100k_base');
console.log(JSON.stringify({encoding:'cl100k_base',tokens:texts.map(text=>encoding.encode(text).length)}));
