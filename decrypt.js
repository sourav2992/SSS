let cachedEncryptionKey = null;

async function getEncryptionKey() {
  if (cachedEncryptionKey) return cachedEncryptionKey;

  const secretResponse = await getSecret(config.get('secrets.chasisSecret'));
  const salt = secretResponse['chassis.secureddatum.encryption.salt'];
  const password = secretResponse['chassis.secureddatum.encryption.password'];

  cachedEncryptionKey = await genEncryptionKey(password, salt);
  // password and salt go out of scope here — nothing retains them
  return cachedEncryptionKey;
}

exports.processOutboundCTR = async function processOutboundCTR(accountReferenceId, customerReferenceId) {
  const encryptionKey = await getEncryptionKey();

  const { escid } = parseReferenceId(decrypt(customerReferenceId, encryptionKey));
  const accountId = getAccountId(accountReferenceId, encryptionKey);
  return { customerId: escid || null, accountId: accountId || null };
};
