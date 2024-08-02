import unittest
from src.storage import S3Bucket
from moto import mock_s3
import boto3
import pytest
from botocore.exceptions import ClientError

class TestS3(unittest.TestCase):

    @mock_s3
    def test_new_bucket_no_name(self):
        new_bucket = S3Bucket()
        try:
            response = new_bucket.client.head_bucket(Bucket=new_bucket.name)
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        except:
            self.fail("Could not create a new S3 bucket without specifying bucket name")

    @mock_s3
    def test_new_bucket_pass_name(self):
        new_bucket = S3Bucket(name="test-bucket", create_new=True)
        self.assertEqual(new_bucket.name, "test-bucket")
        try:
            response = new_bucket.client.head_bucket(Bucket=new_bucket.name)
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        except:
            self.fail("Could not create a new S3 bucket with specifying bucket name")

    @mock_s3
    def test_existing_bucket(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket="test-bucket", CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
        new_bucket = S3Bucket(name="test-bucket", client=client)
        self.assertEqual(new_bucket.name, "test-bucket")
        try:
            response = new_bucket.client.head_bucket(Bucket=new_bucket.name)
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        except:
            self.fail("Could not create a new S3 bucket with specifying bucket name and client")

    @mock_s3
    def test_save(self):
        new_bucket = S3Bucket()
        success = new_bucket.save("tests/logo.png", "logo")
        self.assertTrue(success)
        try:
            response = new_bucket.client.get_object(Bucket=new_bucket.name, Key="logo")
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        except:
            self.fail("Failed to save a file to the bucket")

    @mock_s3
    def test_load(self):
        new_bucket = S3Bucket()
        try:
            object_data = open("tests/logo.png", 'rb')
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo", Body=object_data)
            loaded_obj = new_bucket.load("logo")
            self.assertIsNotNone(loaded_obj)
        except:
            self.fail("Failed to load a file from the bucket")
        finally:
            object_data.close()

    @mock_s3
    @pytest.mark.timeout(3)
    def test_load_or_wait_fail(self):
        new_bucket = S3Bucket(timeout=0.5)

        # test object that exists
        try:
            object_data = open("tests/logo.png", 'rb')
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo", Body=object_data)
            loaded_obj = new_bucket.load_or_wait("logo")
            self.assertIsNotNone(loaded_obj)
        except:
            self.fail("Failed to load a file from the bucket")
        finally:
            object_data.close()

        # test object that doesn't exist
        none_obj = new_bucket.load_or_wait("fake_key")
        self.assertIsNone(none_obj)
        object_data.close()

    @mock_s3
    def test_delete(self):
        new_bucket = S3Bucket()
        try:
            # first save the file
            object_data = open("tests/logo.png", 'rb')
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo", Body=object_data)
            # make sure it is there
            try:
                response = new_bucket.client.get_object(Bucket=new_bucket.name, Key="logo")
                self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
            except:
                self.fail("Failed to save a file to the bucket")
            # test deleting it
            success = new_bucket.delete("logo")
            self.assertTrue(success)
            # make sure it is deleted
            try:
                response = new_bucket.client.get_object(Bucket=new_bucket.name, Key="logo")
                self.fail("Failed to delete the object, the ClientError should have been raised")
            except ClientError as e:
                self.assertEqual(e.response['Error']['Code'], 'NoSuchKey')
        except:
            self.fail("Failed to delete a file from the bucket")
        finally:
            object_data.close()

    @mock_s3
    def test_list(self):
        new_bucket = S3Bucket()
        try:
            # save two files
            object_data = open("tests/logo.png", 'rb')
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo", Body=object_data)
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo2", Body=object_data)

            obj_list = new_bucket.list()
            self.assertEqual(len(obj_list), 2)
        except:
            self.fail("Failed to save files to the bucket")
        finally:
            object_data.close()

    @mock_s3
    def test_clear(self):
        new_bucket = S3Bucket()
        try:
            # first save the files
            object_data = open("tests/logo.png", 'rb')
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo", Body=object_data)
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo2", Body=object_data)
            # make sure it is there
            try:
                response = new_bucket.client.get_object(Bucket=new_bucket.name, Key="logo")
                self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
            except:
                self.fail("Failed to save a file to the bucket")
            # clear
            success = new_bucket.clear()
            self.assertTrue(success)
            # make sure it is deleted
            try:
                response = new_bucket.client.get_object(Bucket=new_bucket.name, Key="logo")
                self.fail("Failed to delete the object, the ClientError should have been raised")
            except ClientError as e:
                self.assertEqual(e.response['Error']['Code'], 'NoSuchKey')
        except:
            self.fail("Failed to delete a file from the bucket")
        finally:
            object_data.close()

    @mock_s3
    def test_bucket_delete(self):
        new_bucket = S3Bucket()
        try:
            # first save the files
            object_data = open("tests/logo.png", 'rb')
            new_bucket.client.put_object(Bucket=new_bucket.name, Key="logo", Body=object_data)
            # make sure it is there
            try:
                response = new_bucket.client.get_object(Bucket=new_bucket.name, Key="logo")
                self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
            except:
                self.fail("Failed to save a file to the bucket")
        except:
            self.fail("Failed to save a file to the bucket")
        finally:
            object_data.close()

        new_bucket.clear(delete_bucket=True)
        # make sure the bucket is deleted
        try:
            response = new_bucket.client.head_bucket(Bucket=new_bucket.name)
            self.fail("Bucket still exists")
        except ClientError as e:
            self.assertEqual(e.response['Error']['Code'], '404')
