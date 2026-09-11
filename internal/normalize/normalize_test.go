package normalize

import "testing"

func TestText(t *testing.T) {
	cases := map[string]string{
		"  Compra   por $350 en  OXXO \n": "Compra por $350 en OXXO",
		"Compra​por $350":                 "Comprapor $350",
		"\t\n":                            "",
		"Terminación 1234　el 10/09/2026":  "Terminación 1234 el 10/09/2026",
	}
	for in, want := range cases {
		if got := Text(in); got != want {
			t.Errorf("Text(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClip(t *testing.T) {
	if got := Clip("Terminación", 5); got != "Termi" {
		t.Errorf("Clip = %q", got)
	}
	if got := Clip("corto", 10); got != "corto" {
		t.Errorf("Clip unchanged = %q", got)
	}
}

func TestFingerprint(t *testing.T) {
	a := Fingerprint("u1", "BBVA", "Compra por $350", "2026-09-10")
	b := Fingerprint("u1", "bbva", "compra  por $350 ", "2026-09-10")
	if a != b {
		t.Errorf("case and spacing must not change the fingerprint")
	}
	if a == Fingerprint("u1", "BBVA", "Compra por $350", "2026-09-11") {
		t.Errorf("another day must differ")
	}
	if a == Fingerprint("u2", "BBVA", "Compra por $350", "2026-09-10") {
		t.Errorf("another user must differ")
	}
	if len(a) != 64 {
		t.Errorf("sha256 hex expected, got %d chars", len(a))
	}
}
