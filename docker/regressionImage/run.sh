#!/bin/bash
docker compose up -d
sleep 10
docker compose logs > out.log
docker compose down 
isInFile=$(cat out.log | grep -c "\[E\]")

if [ $isInFile -eq 0 ]; then
   #string not contained in file
   echo run was successful 
   exit 0
else
   #string is in file at least once
   echo Error exists 
   cat out.log | grep "\[E\]"
   exit 1
fi
