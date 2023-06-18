cd perf_pm

perf record -e cpu-clock -F 60 -g --call-graph=dwarf ./../../build/db_bench --benchmarks="fillseq" --threads=60 --num=10000
mv perf.data perf_pm_fillseq.data
# perf record -e cpu-clock -F 60 -g ./../../build/db_bench --benchmarks="fillrandom" --db="/mnt/pmem0/dbtest"
# mv perf.data perf_pm_fillrandom.data
# perf record -e cpu-clock -F 60 -g ./../../build/db_bench --benchmarks="fillrandom,readseq" --db="/mnt/pmem0/dbtest"
# mv perf.data perf_pm_frreadseq.data
# perf record -e cpu-clock -F 60 -g ./../../build/db_bench --benchmarks="fillrandom,readrandom" --db="/mnt/pmem0/dbtest"
# mv perf.data perf_pm_frreadrandom.data

sudo perf script -i perf_pm_fillseq.data > out_perf_pm_fillseq.perf
# sudo perf script -i perf_pm_fillrandom.data > out_perf_pm_fillrandom.perf
# sudo perf script -i perf_pm_frreadseq.data > out_perf_pm_frreadseq.perf
# sudo perf script -i perf_pm_frreadrandom.data > out_perf_pm_frreadrandom.perf

../../../FlameGraph/stackcollapse-perf.pl out_perf_pm_fillseq.perf > out_perf_pm_fillseq.folded
# ../../../FlameGraph/stackcollapse-perf.pl out_perf_pm_fillrandom.perf > out_perf_pm_fillrandom.folded
# ../../../FlameGraph/stackcollapse-perf.pl out_perf_pm_frreadseq.perf > out_perf_pm_frreadseq.folded
# ../../../FlameGraph/stackcollapse-perf.pl out_perf_pm_frreadrandom.perf > out_perf_pm_frreadrandom.folded

../../../FlameGraph/flamegraph.pl out_perf_pm_fillseq.folded > perf_pm_fillseq.svg
# ../../../FlameGraph/flamegraph.pl out_perf_pm_fillrandom.folded > perf_pm_fillrandom.svg
# ../../../FlameGraph/flamegraph.pl out_perf_pm_frreadseq.folded > perf_pm_frreadseq.svg
# ../../../FlameGraph/flamegraph.pl out_perf_pm_frreadrandom.folded > perf_pm_frreadrandom.svg

cd ../

