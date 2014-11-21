#include <opencv2/opencv.hpp>
#include <iostream>

using namespace std;
using namespace cv;

Mat src;
Size s;
int X;
int Y;
long int totalred = 0;
long int totalgreen = 0;
long int totalblue= 0;
long int total = 0;

 int main( int argc, char** argv )
 {
   /// Load the source image
   src = imread( "~/image.jpg", 1 );
   s = src.size();
   Y = s.height;
   X = s.width;
   total = X*Y;

   for(int x = 0; x < X; ++x) {
      for(int y = 0; y < Y; ++y) {
         Vec3b intensity = src.at<Vec3b>(y, x);
         uchar blue = intensity.val[0];
         uchar green = intensity.val[1];
         uchar red = intensity.val[2];
         totalred += (int)red;
         totalblue += (int)blue;
         totalgreen += (int)green;
//         std::cout << "blue:" << (int)blue << " green:" << (int)green << " red:" << (int)red << std::endl;
      }
   }
   std::cout << "Mean: " << totalred/total << " " << totalgreen/total << " " << totalblue/total << std::endl;
   return 0;
 }

