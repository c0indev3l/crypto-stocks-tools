#!/usr/bin/env bash
DIR=/home/pi/bitcoin/stocks/cryptostocks/

function main(){
                #python ${DIR}main.py --onlywithdividend --onlywithask --days 30 --daysoffset 0 #--maxspreadrel 50
                python ${DIR}main.py --onlywithdividend --days 30 --daysoffset 0 #--maxspreadrel 50
}

#DELAY=3600
DELAY=$[ 30 * 60 ]

while true
do
        main $1
        echo
        date
        msg="Waiting $DELAY s before restarting $0"
        echo $msg
        sleep $DELAY
done
