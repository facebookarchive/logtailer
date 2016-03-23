package sshd

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestKeyParsing(t *testing.T) {
	k, err := ParseAuthorizedKey([]byte(`ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDAVlmAmXcn+mbc0wmWwz52AqSXde7BWkzLhWSrmY+49aZt6chkjYtDz/mTWrTHvJm4kI8SNj4UxmyS8VtofjsE8G5E6E/gVjOtd9q+9Xuv9TdLRjaQPUuXkW+MT+Y1sjShu8e6FzjN1j6IE+z5kYSfB3D96OqVxujof+Oda1ZwDpYO7CyUnna8W169KlJx6miH+uBfICiEHYcH8lt1ATIspcmWUruqc9E827hzroBOgWtInqy7rDZ9ni6S7zcoVxY5NxdvymZPQ1M7jkfy3D+UQmKjelMfC2qqTEn58p234/1RHxI/bSt1UVO3+PSwjr48KsXr1TmJxsbaVdgyDFKCnqRUETM1/q63ceLt06rEueIM3JQq7Yz3CmzlHi6UVOjLb7GFvT0inXihsIYSq5pE3DJv6Lpi/5me1yTuNzJuxXJITnxFaldFgyNzoS/2+0KXxNTh0BSsEXFogy2NLv2/PVo49wqheD2xcfA7+mk9y4qhl1bF3Menyg6ZiPZ9TV1zLEmaSmKBLoOLObG2akPgeshKnG9u4VvA8mqa2NXi7AQka8oqaJGgoFDNoWFsgjhbzKw3tcWWKDD9xjM+jPsEKnr7Dg9c3pKppetQ4YZ81JaM72ZJS1z4nrfeEv+hKuQnDvCrf7Pmh/WWCphKw4/uvNHWrmPPsCnm5JOMrduU8Q== test@fb.com`))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, string(k.Fingerprint()), "b5:ca:16:03:d4:10:41:80:3d:bc:3b:18:05:57:4f:56")
}
