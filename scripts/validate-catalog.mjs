import { products, catalogCategories } from './catalog-source.mjs';

const ids = new Set();
const invalid = products.filter(product => {
  const duplicate = ids.has(product.id); ids.add(product.id);
  const contactPrice = product.price === null;
  const priced = Number.isFinite(product.price) && product.price > 0;
  const validOld = product.old === null || (Number.isFinite(product.old) && product.old > 0);
  return duplicate || !product.id || !product.name || !catalogCategories.includes(product.category) || (!contactPrice && !priced) || !validOld;
});
if (products.length < 48 || invalid.length) throw new Error(`Catalog validation failed: ${products.length} products, ${invalid.length} invalid rows`);
const priced = products.filter(product => Number.isFinite(product.price) && product.price > 0).length;
const contact = products.filter(product => product.price === null).length;
console.log(`Catalog OK: ${products.length} products / ${new Set(products.map(product => product.category)).size} categories / ${ids.size} unique IDs / ${priced} priced / ${contact} contact`);
