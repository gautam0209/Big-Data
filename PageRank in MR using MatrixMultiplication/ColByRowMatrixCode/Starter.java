package cs6240;

public class Starter {
	
	public static void main(String ar[])
	{
		int count =1;
		String arr[] = new String[7];
		arr[0] = ar[0]; arr[1] = ar[1]; arr[2] = ar[2];
		arr[3] = ar[3];
		arr[4] = ar[4];
		arr[5] = Integer.toString(count);
		arr[6] = ar[6];
		long n = 0;
	try{
		
	  	  n =  XMLParser.main(ar);
			System.out.println("RUnning Matrix Creation");
		NodeIndex.main(ar);
		MatrixCreationRC.main(ar,n);
		MatrixCreationR.main(arr,n);
	}
	catch(Exception e) {e.printStackTrace();}
	
	
	while(count <= 10)
	{
	
	try{
		long startTime = System.currentTimeMillis();
		System.out.println("Outside RUnning Matrix" + count);
		arr[5] = Integer.toString(count);
		System.out.println("RUnning Matrix Mul" + count);
		MatrixMul.main(arr,n);
		MatrixMulSum.main(arr,n);
		
		if(count == 1)
			MatrixCreatorD.main(arr,n);
		MatrixMulD.main(arr);
		DanglingSum.main(arr);
		DanglingR.main(arr, n);
		System.out.println("Time for itr " + count + " is:" +( System.currentTimeMillis() - startTime));
		count++;
	}
	catch(Exception e){ e.printStackTrace(); }
	
	}
	
	try{ 
		Top100.main(arr);
	}
	catch(Exception e) {} 
	
	}

}
