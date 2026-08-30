import os
import tempfile
import unittest
from pathlib import Path

from cryptography.fernet import Fernet

from termux.refresh_wizz_direct import AUTH_ERROR_RE, CHALLENGE_RE, _visible_text
from termux.configure_wizz_credentials import save_credentials
from credential_vault import CredentialVault


class UnattendedWizzLoginTests(unittest.TestCase):
    def test_script_bundle_words_do_not_fake_auth_rejection(self):
        html = """
        <html><body>
          <script>const message = 'invalid password'; const challenge='captcha';</script>
          <main>Welcome back</main>
        </body></html>
        """
        text = _visible_text(html)
        self.assertIn("Welcome back", text)
        self.assertIsNone(AUTH_ERROR_RE.search(text))
        self.assertIsNone(CHALLENGE_RE.search(text))

    def test_visible_auth_error_is_detected(self):
        text = _visible_text("<main><p>Invalid password. Please try again.</p></main>")
        self.assertIsNotNone(AUTH_ERROR_RE.search(text))

    def test_visible_security_challenge_is_detected(self):
        text = _visible_text("<main>Enter verification code</main>")
        self.assertIsNotNone(CHALLENGE_RE.search(text))

    def test_secure_setup_writes_reusable_encrypted_vault(self):
        with tempfile.TemporaryDirectory() as tmp:
            old_key = os.environ.get("AYCF_SESSION_ENCRYPTION_KEY")
            old_file = os.environ.get("AYCF_WIZZ_CREDENTIAL_FILE")
            try:
                key = Fernet.generate_key().decode()
                target = Path(tmp) / "credentials.enc"
                os.environ["AYCF_SESSION_ENCRYPTION_KEY"] = key
                os.environ["AYCF_WIZZ_CREDENTIAL_FILE"] = str(target)
                save_credentials("user@example.com", "secret-password")
                self.assertTrue(target.exists())
                self.assertNotIn(b"secret-password", target.read_bytes())
                self.assertEqual(
                    CredentialVault().load(),
                    {"username": "user@example.com", "password": "secret-password"},
                )
            finally:
                if old_key is None:
                    os.environ.pop("AYCF_SESSION_ENCRYPTION_KEY", None)
                else:
                    os.environ["AYCF_SESSION_ENCRYPTION_KEY"] = old_key
                if old_file is None:
                    os.environ.pop("AYCF_WIZZ_CREDENTIAL_FILE", None)
                else:
                    os.environ["AYCF_WIZZ_CREDENTIAL_FILE"] = old_file


if __name__ == "__main__":
    unittest.main()
