# fareEstimator
Overview for the beat fare estimation golang script

The basic architectural design is that this script reads in a go routine the input file and for every valid segment of the same id puts content in a channel.

The fare is calculated concurrently for each valid segment and is stacked up in map per ride id. A secondary channel is used of signaling when micro fare calculation is done per each segment

Finally we wait for the secondary signal channel to empty and we calculate the final fare for each id as instructed by the assignment.
