Generate Amazon SageMaker Ground Truth augmented manifest files from image and text files in Amazon S3

```
Usage: generate-manifest.py <object_type:('image' or 'text')> <input_s3_bucket> <output_s3_bucket>
```
## Examples 
### Generating manifest.json for image files in S3
```
python generate-manifest.py "image" s3://<s3_bucket>/images s3://<s3_bucket>/manifest
```

### Generating manifest.json for text in csv files in S3
```
python generate-manifest.py "text" s3://<s3_bucket>/text s3://<s3_bucket>/manifest
```