# Private Key Authentication Setup Guide

## Quick Answer

**Add the path to your private key file in the `.env` file:**

```bash
cd /Users/beatrice/Documents/anais_ingestion/DBT/anais_staging

# Create .env file
cat > .env << 'EOF'
SFTP_HOST="your.sftp.host"
SFTP_PORT=22
SFTP_USERNAME="your_username"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
EOF

# Optional: If your key is encrypted with a passphrase
cat >> .env << 'EOF'
SFTP_PRIVATE_KEY_PASSPHRASE="your_key_passphrase"
EOF

# Protect the .env file
chmod 600 .env
```

## Step-by-Step Setup

### Step 1: Locate Your Private Key

Common locations:
```bash
# RSA key
~/.ssh/id_rsa

# Ed25519 key (newer, more secure)
~/.ssh/id_ed25519

# ECDSA key
~/.ssh/id_ecdsa

# Custom location
/path/to/your/private_key
```

**Find your key:**
```bash
ls -l ~/.ssh/
```

### Step 2: Verify Key Permissions

Your private key must have restrictive permissions:

```bash
# Check permissions
ls -l ~/.ssh/id_rsa

# Should show: -rw------- (600)
# If not, fix it:
chmod 600 ~/.ssh/id_rsa
```

### Step 3: Create .env File

**Option A: Using the example template**
```bash
cd /Users/beatrice/Documents/anais_ingestion/DBT/anais_staging

# Copy example
cp .env.example .env

# Edit with your values
nano .env
```

**Option B: Create from scratch**
```bash
cd /Users/beatrice/Documents/anais_ingestion/DBT/anais_staging

cat > .env << 'EOF'
# SFTP Server Configuration
SFTP_HOST="your.sftp.server.com"
SFTP_PORT=22
SFTP_USERNAME="your_username"

# Private Key Authentication
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"

# Optional: Only if your key is encrypted
# SFTP_PRIVATE_KEY_PASSPHRASE="your_passphrase"
EOF

# Protect the file
chmod 600 .env
```

### Step 4: Test Connection

```bash
cd /Users/beatrice/Documents/anais_ingestion/DBT/anais_staging

# Run pipeline with SFTP
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```

**Check logs:**
```bash
cat logs/log_local_sftp.log | grep -A 5 "SFTP"
```

You should see:
```
Connecting with private key authentication...
Trying to load RSA private key from /Users/beatrice/.ssh/id_rsa
✅ SFTP connection established with private key
```

## Supported Key Types

The script automatically detects and supports:

| Key Type | File Example | Algorithm |
|----------|--------------|-----------|
| **RSA** | `id_rsa` | Traditional, widely compatible |
| **Ed25519** | `id_ed25519` | Modern, fast, secure (recommended for new keys) |
| **ECDSA** | `id_ecdsa` | Elliptic curve |

## Authentication Methods

### Method 1: Private Key (Recommended)

```env
SFTP_HOST="sftp.example.com"
SFTP_PORT=22
SFTP_USERNAME="john"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
```

**Pros:**
- ✅ More secure than passwords
- ✅ No password in plain text
- ✅ Can be used with SSH agent
- ✅ Industry standard

### Method 2: Private Key with Passphrase

```env
SFTP_HOST="sftp.example.com"
SFTP_PORT=22
SFTP_USERNAME="john"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
SFTP_PRIVATE_KEY_PASSPHRASE="my_secure_passphrase"
```

**When to use:** If your private key is encrypted (has a passphrase)

### Method 3: Password (Fallback)

```env
SFTP_HOST="sftp.example.com"
SFTP_PORT=22
SFTP_USERNAME="john"
SFTP_PASSWORD="my_password"
```

**Only used if:** `SFTP_PRIVATE_KEY_PATH` is not set

## Common Issues and Solutions

### Issue 1: "Private key file not found"

```
FileNotFoundError: Private key file not found: /Users/beatrice/.ssh/id_rsa
```

**Solution:**
```bash
# Check if file exists
ls -l ~/.ssh/id_rsa

# If not, check for other key types
ls -l ~/.ssh/id_*

# Update .env with correct path
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_ed25519"  # Use actual key file
```

### Issue 2: "Could not load private key"

```
ValueError: Could not load private key from /Users/beatrice/.ssh/id_rsa
```

**Possible causes:**
1. **Wrong file format** - File is not actually a private key
2. **Encrypted key without passphrase** - Key has passphrase but not provided
3. **Corrupted key file**

**Solutions:**
```bash
# Check file type
file ~/.ssh/id_rsa
# Should show: "OpenSSH private key" or "PEM RSA private key"

# Test key manually
ssh -i ~/.ssh/id_rsa username@sftp.host
# If it asks for passphrase, add to .env:
SFTP_PRIVATE_KEY_PASSPHRASE="your_passphrase"

# Generate new key if corrupted
ssh-keygen -t ed25519 -C "your_email@example.com"
```

### Issue 3: "Permission denied (publickey)"

```
paramiko.ssh_exception.AuthenticationException: Authentication failed
```

**Possible causes:**
1. Public key not installed on SFTP server
2. Wrong username
3. Server doesn't accept this key type

**Solutions:**
```bash
# 1. Copy public key to server
ssh-copy-id -i ~/.ssh/id_rsa.pub username@sftp.host

# 2. Manually add public key to server
cat ~/.ssh/id_rsa.pub
# Then add to server: ~/.ssh/authorized_keys

# 3. Verify username
# Check .env file for correct SFTP_USERNAME

# 4. Test SSH connection manually
ssh -i ~/.ssh/id_rsa username@sftp.host
```

### Issue 4: "Bad permissions"

```
WARNING: UNPROTECTED PRIVATE KEY FILE!
```

**Solution:**
```bash
# Fix file permissions
chmod 600 ~/.ssh/id_rsa
chmod 600 .env

# Fix directory permissions
chmod 700 ~/.ssh
```

## Security Best Practices

### 1. Protect Your Private Key

```bash
# Correct permissions (read/write for owner only)
chmod 600 ~/.ssh/id_rsa

# Verify
ls -l ~/.ssh/id_rsa
# Should show: -rw-------
```

### 2. Protect Your .env File

```bash
chmod 600 .env

# Verify it's in .gitignore
git check-ignore .env
# Should output: .env
```

### 3. Use Ed25519 Keys (For New Keys)

```bash
# Generate new Ed25519 key (more secure, faster)
ssh-keygen -t ed25519 -C "your_email@example.com" -f ~/.ssh/anais_sftp

# Update .env
SFTP_PRIVATE_KEY_PATH="~/.ssh/anais_sftp"
```

### 4. Use Key Passphrases

When generating a new key, always add a passphrase:

```bash
ssh-keygen -t ed25519 -C "your_email@example.com"
# Enter passphrase when prompted
```

Then add passphrase to .env:
```env
SFTP_PRIVATE_KEY_PASSPHRASE="your_passphrase"
```

### 5. Use Separate Keys for Different Services

```bash
# Generate key specifically for ANAIS SFTP
ssh-keygen -t ed25519 -C "anais-sftp" -f ~/.ssh/anais_sftp

# Use in .env
SFTP_PRIVATE_KEY_PATH="~/.ssh/anais_sftp"
```

## Testing Your Setup

### Manual SSH Test

Before using the pipeline, test SSH connection manually:

```bash
# Test with your key
ssh -i ~/.ssh/id_rsa username@sftp.host

# Test with verbose output (for debugging)
ssh -v -i ~/.ssh/id_rsa username@sftp.host
```

### Pipeline Test

```bash
cd /Users/beatrice/Documents/anais_ingestion/DBT/anais_staging

# Dry run to test connection (downloads files but doesn't process)
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp

# Check logs for connection details
tail -50 logs/log_local_sftp.log
```

### Expected Log Output

**Success:**
```
================================================================================
📥 STEP 1: Downloading files from SFTP...
================================================================================
Connecting with private key authentication...
Trying to load RSA private key from /Users/beatrice/.ssh/id_rsa
✅ SFTP connection established with private key
Connexion SFTP établie.
```

**Failure:**
```
Connecting with private key authentication...
Trying to load RSA private key from /Users/beatrice/.ssh/id_rsa
❌ SFTP connection failed: [Errno 2] No such file or directory: '/Users/beatrice/.ssh/id_rsa'
```

## Complete Example

Here's a complete working example:

```bash
# 1. Navigate to project
cd /Users/beatrice/Documents/anais_ingestion/DBT/anais_staging

# 2. Check your SSH key
ls -l ~/.ssh/id_rsa

# 3. Fix permissions if needed
chmod 600 ~/.ssh/id_rsa

# 4. Create .env file
cat > .env << 'EOF'
SFTP_HOST="sftp.example.com"
SFTP_PORT=22
SFTP_USERNAME="beatrice"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
EOF

# 5. Protect .env
chmod 600 .env

# 6. Test connection manually (optional)
ssh -i ~/.ssh/id_rsa beatrice@sftp.example.com

# 7. Run pipeline
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp

# 8. Check results
ls -lh input/staging/
cat logs/log_local_sftp.log
```

## Summary

**Location for private key path:** `DBT/anais_staging/.env`

**Variable name:** `SFTP_PRIVATE_KEY_PATH`

**Supported paths:**
- `~/.ssh/id_rsa` (tilde expansion supported)
- `/Users/beatrice/.ssh/id_rsa` (absolute path)
- `../keys/my_key` (relative path)

**Supported key types:** RSA, Ed25519, ECDSA

**Optional:** `SFTP_PRIVATE_KEY_PASSPHRASE` (only if key is encrypted)

## Quick Reference Card

```bash
# File location
/Users/beatrice/Documents/anais_ingestion/DBT/anais_staging/.env

# Required variables
SFTP_HOST="..."
SFTP_PORT=22
SFTP_USERNAME="..."
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"

# Optional variables
SFTP_PRIVATE_KEY_PASSPHRASE="..."  # Only if key is encrypted

# File permissions
chmod 600 .env
chmod 600 ~/.ssh/id_rsa

# Run command
cd DBT/anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```
