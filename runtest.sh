
#echo "=============== 100.000 ============"
#python3 main.py $1 write 100000

echo "=============== 10.000 ============"
python3 main.py $1 read 10000
echo "=============== 20.000 ============"
python3 main.py $1 read 20000
echo "=============== 50.000 ============"
python3 main.py $1 read 50000
echo "=============== 100.000 ============"
python3 main.py $1 read 100000
echo "=============== 200.000 ============"
python3 main.py $1 read 200000
