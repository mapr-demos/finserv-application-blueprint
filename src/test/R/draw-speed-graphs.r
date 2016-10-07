# This script generates boxplot figures for test data. Boxplots are described here:
#   https://en.wikipedia.org/wiki/Box_plot
#   https://stat.ethz.ch/R-manual/R-devel/library/graphics/html/boxplot.html


png(file="topics.png", width=800, height=500, pointsize=16)
x = read.csv("topic-count.csv")
boxplot(batchRate/1e6 ~ topicCount + batchSize, x[x$i>2e6,], 
        xlab=c("Topics"), ylab="Millions of Messages / second", ylim=c(0,2),
        col=rainbow(3)[ceiling((1:9)/3)], xaxt='n')
axis(1,labels=as.character(rep(c(100,300,1000),3)), at=(1:9), las=3)
legend(x=8,y=1.9,legend=c(0,16384,65536), col=rainbow(3), fill=rainbow(3), title="batch.size")
abline(v=3.5, col='lightgray')
abline(v=6.5, col='lightgray')
dev.off()



png(file="thread.png", width=800, height=500, pointsize=16)
x = read.csv("thread-count.csv")
boxplot(batchRate/1e6 ~ topicCount + threadCount, x, ylim=c(0,2.1), 
        ylab="Millions of messages / second", xlab="Topics",
        col=rainbow(4)[ceiling((1:20)/5)], xaxt='n')
axis(1,labels=as.character(rep(c(50,100,200,500,1000),4)), at=(1:20), las=3)
legend(x=17,y=2.1,legend=c(1,2,5,10), col=rainbow(4), fill=rainbow(4), title="Threads")
abline(v=5.5, col='lightgray')
abline(v=10.5, col='lightgray')
abline(v=15.5, col='lightgray')
dev.off()
