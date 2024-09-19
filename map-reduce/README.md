# Word Count with Hadoop MapReduce on Private Chivalry Text

This project demonstrates a word count example using Hadoop MapReduce with a text file (`private_chivarly.txt`) downloaded from Project Gutenberg. The process involves setting up a Hadoop environment within a Vagrant virtual machine, copying the text file to HDFS, and running a MapReduce job to perform word counting on the text.

## Steps to Execute

### Part 1: Downloading the Text File

We downloaded a book in plain text format from Project Gutenberg:

- **Source URL**: [Private Chivalry Text](https://www.gutenberg.org/cache/epub/72849/pg72849.txt)
  
To download the file inside the Vagrant virtual environment, run the following command:

```bash
$ wget https://www.gutenberg.org/cache/epub/72849/pg72849.txt -O private_chivarly.txt
