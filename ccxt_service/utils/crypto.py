import base64
import os
from typing import Tuple

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

PRIVATE_KEY_FILE = "private_key.pem"
PUBLIC_KEY_FILE = "public_key.pem"


def generate_keys() -> Tuple[str, str]:
    if os.path.exists(PRIVATE_KEY_FILE) and os.path.exists(PUBLIC_KEY_FILE):
        with open(PRIVATE_KEY_FILE, "r") as f:
            private_pem = f.read()
        with open(PUBLIC_KEY_FILE, "r") as f:
            public_pem = f.read()
        return private_pem, public_pem

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()

    with open(PRIVATE_KEY_FILE, "w") as f:
        f.write(private_pem)
    with open(PUBLIC_KEY_FILE, "w") as f:
        f.write(public_pem)

    return private_pem, public_pem


def encode_key(encrypted: str | bytes, private_key_pem: str) -> str:
    if isinstance(encrypted, str):
        encrypted_bytes = base64.b64decode(encrypted)
    else:
        encrypted_bytes = encrypted

    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(), password=None
    )

    decrypted = private_key.decrypt(
        encrypted_bytes,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return decrypted.decode("utf-8")


PRIVATE_KEY, PUBLIC_KEY = generate_keys()
