package org.shirdrn.streaming.agent;
import java.io.File;
import java.io.RandomAccessFile;


public class LogAppendingClient {

	public static void main(String[] args) throws Exception {
		String path = "F:\\test\\basis_behavior_user\\2014-05-28_18-03.log";
		File file = new File(path);
		RandomAccessFile fileHandle = new RandomAccessFile(file, "rw");
		
		String[] lines = new String[] {
				"aaaaef93ce1249d988a412c01d2ee8b5	100000000385322	1401271380552	8548aad94fdf4fca838fa7067948dba5	1	0	Android	4.2	2.2.1	wifi		Spreadtrum	92	800*480	E-sum8	00:27:15:42:17:9f	865583013020333	8548aad94fdf4fca838fa7067948dba51401271228948	460008251250817	2051642496	1401271380173	31	749	55923	2	3	0	2014-05-28 18:03:00	3	3	3	",
				"bbbbb91131c41e29783f768f7dc5cbd	4818d819db0163a732bd4f3dfbf98587	1401271380988	4818d819db0163a732bd4f3dfbf98587	0	0	GT-I9508	4.3	2.2.1	wifi		samsung	39	1920*1080	A-360	40:0E:85:0E:04:26	355637055801088	4818d819db0163a732bd4f3dfbf985871401271191377	460021505033676	3745628079	1401271379589	31	747	56567	2	3	0	2014-05-28 18:03:00	3	3	3	",
				"ddddab5b15841beb89d21e03b3b6956	eac800bbfd2d8d7791969294ddaf148d	1401201480467	eac800bbfd2d8d7791969294ddaf148d	0	0	GT-I9308	4.1.2	2.1.7	wifi		samsung	384	1280*720	A-baidu	38:AA:3C:D7:56:D5	352343051029930	eac800bbfd2d8d7791969294ddaf148d1401200954000	460078069877336	248267102	1401201490307	429	84	19969	2	3	0	2014-05-27 22:38:00	3	3	3	",
				"eeee695e3cf8462e87066a42274fa175	430783c3d42781bca2f7e9b03a22738d	1401201509318	430783c3d42781bca2f7e9b03a22738d	0	0	SCH-I699	2.3.6	2.1.2	wifi		samsung	10	800*480	H-tangmen27	64:77:91:6A:B1:12	A000004089FB8E	430783c3d42781bca2f7e9b03a22738d1401201501111	460030427446455	1946252002	1401201525254	429	177	20066	2	3	0	2014-05-27 22:38:29	3	3	3	",
				"ffffe2f01c0641b38dbe5df1277cbf4a	eac800bbfd2d8d7791969294ddaf148d	1401201510900	eac800bbfd2d8d7791969294ddaf148d	0	0	GT-I9308	4.1.2	2.1.7	wifi		samsung	408	1280*720	A-baidu	38:AA:3C:D7:56:D5	352343051029930	eac800bbfd2d8d7791969294ddaf148d1401200954000	460078069877336	248267102	1401201526223	429	235	20047	2	3	0	2014-05-27 22:38:30	3	3	3	"
		};
		
		fileHandle.seek(fileHandle.length());
		
		for (int i = 0; i < lines.length; i++) {
			fileHandle.writeBytes(lines[i] + "\\r\\n");
			fileHandle.getChannel().force(false);
			System.out.println("Write: " + lines[i]);
			Thread.sleep(5000);
		}
		
		fileHandle.close();
		System.out.println("file colsed!");
	}

}
