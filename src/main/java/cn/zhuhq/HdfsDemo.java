package cn.zhuhq;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HdfsDemo {
    
//	连接hadoop集群，获取集群的文件系统
	public FileSystem getClient() throws IOException, InterruptedException, URISyntaxException {
		// TODO Auto-generated constructor stub
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.102:9000"), conf , "hadoop");
		
		System.out.println("Connect to hadoop cluster successfully!");
		return fs;
	}
	
//	创建目录
	public void makeDir() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		Path path = new Path("/zhuhq/xxx");
		boolean mkdirs = client.mkdirs(path);
		if (mkdirs) {
			System.out.println("You have made the dir "+path+" succesfully!");
		}else {
			System.out.println("You have made the dir "+path+" failed!");
		}
			client.close();
			
		
	}
//	删除目录	
	public void DelDir() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		Path path = new Path("/zhuhq/testz");
		boolean delete = client.delete(path, true);
		if (delete) {
			System.out.println("You have deleted the dir "+path+" succesfully!" );
		} else {
			System.out.println("You have deleted the dir "+path+" failed!" );
		}
	}
	
//	上传文件
	public void uploadFile() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		Path dst = new Path("/zhuhq/test/");
		Path src = new Path("e:/study/hello.txt");
		client.copyFromLocalFile(src, dst);
		System.out.println("the file "+src+" is uploading to "+dst+" succesfuly!");
		client.close();
	}
	
// 下载文件
	public void downloadFile() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		
		Path dst = new Path("e:/study");
		Path src = new Path("/usr/hadoop/input/wc.input");
		client.copyToLocalFile(src, dst);
		System.out.println("the file "+src+" is downloading to "+dst+" succesfuly!");
		client.close();
	}
//	文件名更改
	public void renameFile() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		
		Path dst = new Path("/zhuhq/test/test.txt");
		Path src = new Path("/zhuhq/test/hello.txt");
		boolean rename = client.rename(src, dst);
		if (rename) {
			System.out.println("the file "+src+" is renamed to "+dst+" succesfully!");
		} else {
			System.out.println("the file "+src+" is renamed to "+dst+" failed!");
		}
		client.close();
	}
//	获取hdfs文件信息
	public void getListFileInfo() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		
		RemoteIterator<LocatedFileStatus> listFiles = client.listFiles(new Path("/"), true);
		
		while (listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			
//			输出文件详情
//			文件名称
			System.out.println(status.getPath().getName());
//			长度
			System.out.println(status.getLen());
//			权限
			System.out.println(status.getPermission());
//			分组
			System.out.println(status.getGroup());
			
//			获取存储块信息
			
			BlockLocation[] locations = status.getBlockLocations();
			for (BlockLocation blockLocation : locations) {
//				获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("-------------------班长的分割线-----------------------------------");
		}
		client.close();
	}
//	判断hdfs文件和文件夹
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException {
		FileSystem client = this.getClient();
		FileStatus[] listStatus = client.listStatus(new Path("/zhuhq/test"));
		for (FileStatus fileStatus : listStatus) {
			if (fileStatus.isFile()) {
				System.out.println("f:"+fileStatus.getPath().getName());
			} else {
				System.out.println("d:"+fileStatus.getPath().getName());
			}
		}
	}
//	通过I/O流上传文件到hdfs
	public void putFile2Hdfs() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
//		获得hdfs文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.102:9000"), conf , "hadoop");
//        创建输入流
		FileInputStream fin = new FileInputStream(new File("e:/study/test.txt"));
//		获取hdfs输出流
		FSDataOutputStream fout = fs.create(new Path("/zhuhq/test/test.txt"));
//		流对拷贝
		IOUtils.copyBytes(fin, fout, conf);
//		关闭资源
		IOUtils.closeStream(fin);
		IOUtils.closeStream(fout);
		fs.close();
	}
//	通过I/O流从hdfs下载文件到本地
	public void getFileFromHdfs() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
//		获得hdfs文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.102:9000"), conf , "hadoop");
//      获取hdfs输入流
		FSDataInputStream fin = fs.open(new Path("/usr/hadoop/input/wc.input"));
//		获取输出流
		 FileOutputStream fout = new FileOutputStream("e:/study/wc.input");
//		流对拷贝
		IOUtils.copyBytes(fin, fout, conf);
//		关闭资源
		IOUtils.closeStream(fin);
		IOUtils.closeStream(fout);
		fs.close();
	}
//	定位读取文件
//	下载第一块
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.102:9000"), configuration, "hadoop");
			
		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/zhuhq/test/hadoop-2.7.2.tar.gz"));
			
		// 3 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/study/hadoop-2.7.2.tar.gz.part1"));
			
		// 4 流的拷贝
		byte[] buf = new byte[1024];
			
		for(int i =0 ; i < 1024 * 128; i++){
			fis.read(buf);
			fos.write(buf);
		}
			
		// 5关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	fs.close();
	}
//	读第二块
	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.102:9000"), configuration, "hadoop");
			
		// 2 打开输入流
		FSDataInputStream fis = fs.open(new Path("/zhuhq/test/hadoop-2.7.2.tar.gz"));
			
		// 3 定位输入数据位置
		fis.seek(1024*1024*128);
			
		// 4 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("e:/study/hadoop-2.7.2.tar.gz.part2"));
			
		// 5 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);
			
		// 6 关闭资源
		IOUtils.closeStream(fis);
		IOUtils.closeStream(fos);
	}
}
