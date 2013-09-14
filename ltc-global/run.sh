#!/usr/bin/env bash
DIR=/home/pi/bitcoin/stocks/ltc-global/

function main(){
                python ${DIR}main.py --days 30 --daysoffset 0 #--maxspreadrel 30 --onlywithdividend --onlywithask
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
