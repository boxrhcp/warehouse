#!/bin/sh
#set -x

RES=`echo "$1" | grep -E '^ssoo_p3(_100[[:digit:]]{6}){1,2}.zip$'`

if [ ! -f "$1" ] ;then
	echo "File does not exist"
	exit
fi

if [ "$RES" = "" ] ;then
	echo "Incorrect file name/format"
	exit
fi

echo "Correct file name/format"

ZIP=$(basename "$1")
ZIP="${ZIP%.*}"
unzip -q $1 -d $ZIP
echo $ZIP
cp concurrent_example.c $ZIP
cp -r include $ZIP
cp -r lib $ZIP
cp Makefile $ZIP
cp sequential.c $ZIP
cp sequential_example.c $ZIP
cd $ZIP

if [ ! -e concurrent.c -o ! -e autores.txt ]; then
	echo "One or more of the files were not found"
fi

make clean 2> /dev/null > /dev/null
make       2> /dev/null > /dev/null

if [ ! -e concurrent_example.exe ]; then
	echo "Compilation error"
else
	echo "Compilation OK"
fi

./concurrent_example.exe

cd ..
rm -r $ZIP
exit



