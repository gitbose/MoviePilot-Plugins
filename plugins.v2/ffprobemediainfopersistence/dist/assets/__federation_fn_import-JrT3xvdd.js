const moduleCache = Object.create(null);

async function importShared(name, shareScope = 'default') {
  if (moduleCache[name]) return moduleCache[name];
  const versions = globalThis.__federation_shared__?.[shareScope]?.[name];
  if (!versions) throw new Error(`Shared module not available: ${name}`);
  const versionKey = Object.keys(versions)[0];
  const module = await (await versions[versionKey].get())();
  const flattened = module.default
    ? Object.assign({}, module.default, module)
    : module;
  moduleCache[name] = flattened;
  return flattened;
}

export { importShared };
