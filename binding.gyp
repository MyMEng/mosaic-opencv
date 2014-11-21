{
  "targets": [
    {
      "target_name": "cv",
      "sources": [ "main.cpp" ],
	  "include_dirs": [ "/usr/local/include/" ],  
      "link_settings": {
                        'libraries':    ['-lopencv_core -lopencv_features2d -lopencv_contrib'],
                        'library_dirs': ['/usr/local/lib/'],
                       },
    },
    {
      "target_name": "mean",
      "sources": [ "mean.cpp" ],
          "include_dirs": [ "/usr/local/include/" ],
      "link_settings": {
                        'libraries':    ['-lopencv_core -lopencv_features2d -lopencv_contrib'],
                        'library_dirs': ['/usr/local/lib/'],
                       },
    }
  ]
}
