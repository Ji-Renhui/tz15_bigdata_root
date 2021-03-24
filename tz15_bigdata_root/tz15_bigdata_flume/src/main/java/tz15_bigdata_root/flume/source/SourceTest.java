//package tz15_bigdata_root.flume.source;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.FileUtils;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Collection;
//import java.util.List;
//
//import static java.io.File.separator;
//import java.io.File;
//import java.util.Collection;
//
///**
// * @ClassName SourceTest
// * @Description
// * @Date 2021/1/25 16:32
// * @Created by robot
// **/
//public class SourceTest {
//    public static void main(String[] args) {
//       String foldDir="E:\\项目测试数据\\wechat";
//        Collection<File> files = FileUtils.listFiles(new File(foldDir), new String[]{"txt"}, true);
//    if(files.size()>0){
//        files.forEach(file -> {
//
//            String filename=file.getName();
//            String succDir="E:\\项目测试数据\\succ";
//            String succDirN=succDir + separator+"20210125";
//            String absouluteDir=succDir+filename;
//            try {
//                if(new File(absouluteDir).exists()) {
//                    //不处理
//                }else {
//                    List<String> lines = FileUtils.readLines(file);
//                    lines.forEach(line->{
//                        System.out.println("数据内容=>    "+line);
//                    });
//                    //备份
//                    FileUtils.moveToDirectory(file,new File(succDirN),true);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        });
//    }
//
//    }
//}
