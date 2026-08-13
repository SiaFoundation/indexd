---
default: patch
---

# Speed up pruning

#1058 by @chris124567

Before:
```
goos: linux
goarch: amd64
pkg: go.sia.tech/indexd/persist/postgres
cpu: Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz
BenchmarkPruneSlabsLargeAccount-4             2    24040726443 ns/op    15000 slabs/op
PASS
ok      go.sia.tech/indexd/persist/postgres    53.039s
```

After:
```
goos: linux
goarch: amd64
pkg: go.sia.tech/indexd/persist/postgres
cpu: Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz
BenchmarkPruneSlabsLargeAccount-4             2     3542584632 ns/op    15000 slabs/op
PASS
ok      go.sia.tech/indexd/persist/postgres    12.583s
```

Close #1055
