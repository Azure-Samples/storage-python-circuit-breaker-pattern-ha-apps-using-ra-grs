# ----------------------------------------------------------------------------------
# Microsoft Developer & Platform Evangelism
# Copyright (c) Microsoft Corporation. All rights reserved.
# THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
# ----------------------------------------------------------------------------------
# The example companies, organizations, products, domain names,
# e-mail addresses, logos, people, places, and events depicted
# herein are fictitious.  No association with any real company,
# organization, product, domain name, email address, logo, person,
# places, or events is intended or should be inferred.
# ----------------------------------------------------------------------------------

import os
import uuid
import time
import sys
from azure.storage.blob import BlockBlobService
from azure.storage.common.models import LocationMode
from azure.storage.common.retry import LinearRetry

'''
Azure Storage Circuit Breaker Demo
INSTRUCTIONS
Please see the README.md file for an overview explaining this application and how to run it.
'''
account_name = "storageaccountragrs"
account_key = "3k1YHiyxsME2YC52FcQQk+Yj+8yX5uYCVydPUP2rN4WTUokFODWzwRU9cF6qxWGS7NaFm1UFuinMAiCE1jSakg=="

# Track how many times retry events occur.
retry_count = 0  # Number of retries that have occurred
retry_threshold = 5  # Threshold number of retries before switching to secondary
secondary_read_count = 0  # Number of reads from secondary that have occurred
secondary_threshold = 20  # Threshold number of reads from secondary before switching back to primary

# This is the CloudBlobClient object used to access the blob service
blob_client = None

# This is the container used to store and access the blob to be used for testing
container_name = None

'''
Main method. Sets up the objects needed, the performs a loop to perform blob
 operation repeatedly, responding to the Retry and Response Received events.
'''


def run_circuit_breaker():
    # Name of image to use for testing.
    image_to_upload = "HelloWorld.png"

    global blob_client
    global container_name
    try:

        # Create a reference to the blob client and container using the storage account name and key
        blob_client = BlockBlobService(account_name, account_key)

        # Make the container unique by using a UUID in the name.
        container_name = "democontainer" + str(uuid.uuid4())
        blob_client.create_container(container_name)

    except Exception as ex:
        print("Please make sure you have put the correct storage account name and key.")
        print(ex)

    # Define a reference to the actual blob and upload the block_blob to the newly created container
    full_path_to_file = os.path.join(os.path.dirname(__file__), image_to_upload)
    blob_client.create_blob_from_path(container_name, image_to_upload, full_path_to_file)

    # Set the location mode to secondary, so you can check just the secondary data center.
    blob_client.location_mode = LocationMode.SECONDARY
    blob_client.retry = LinearRetry(backoff=0).retry

    # Before proceeding, wait until the blob has been replicated to the secondary data center.
    # Loop and check for the presence of the blob once in a second until it hits 60 seconds
    # or until it finds it
    counter = 0
    while counter < 60:
        counter += 1
        sys.stdout.write("\nAttempt {0} to see if the blob has replicated to the secondary storage yet.".format(counter))
        sys.stdout.flush()
        if blob_client.exists(container_name, image_to_upload):
            break

        # Wait a second, then loop around and try again
        # When it's finished replicating to the secondary, continue.
        time.sleep(1)

    '''
    Set the starting LocationMode to Primary, then Secondary.
    Here we use the linear retry by default, but allow it to retry to secondary if
    the initial request to primary fails.
    Note that the default is Primary. You must have RA-GRS enabled to use this
    '''

    blob_client.location_mode = LocationMode.PRIMARY
    blob_client.retry = LinearRetry(max_attempts=retry_threshold, backoff=1).retry

    ''' 
        ************INSTRUCTIONS**************k
        To perform the test, first replace the 'accountname' and 'accountkey' with your storage account name and key.
        Every time it calls get_blob_to_path it will hit the response_callback function.

        Next, run this app. While this loop is running, pause the program by pressing any key, and
        put the intercept code in Fiddler (that will intercept and return a 503).

        For or instructions on modifying Fiddler, look at the Fiddler_script.text file in this project
        There are also full instructions in the ReadMe_Instructions.txt file included in this project

        After adding the custom script to Fiddler, calls to primary storage will fail with a retryable
        error which will trigger the Retrying event (above).
        Then it will switch over and read the secondary. It will do that 20 times, then try to
        switch back to the primary
        After seeing that happen, pause this again and remove the intercepting Fiddler code
        Then you'll see it return to the primary and finish.
        '''

    print("\n\nThe application will pause at 200 unit interval")

    for i in range(0, 1000):
        if blob_client.location_mode == LocationMode.SECONDARY:
            sys.stdout.write("S{0} ".format(str(i)))
        else:
            sys.stdout.write("P{0} ".format(str(i)))
        sys.stdout.flush()

        try:

            blob_client.retry_callback = retry_callback
            blob_client.response_callback = response_callback

            blob_client.get_blob_to_path(container_name, image_to_upload,
                                                str.replace(full_path_to_file, ".png", "Copy.png"))

            if i == 200 or i == 400 or i == 600 or i == 800:
                sys.stdout.write("\nPress the Enter key to resume")
                sys.stdout.flush()
                raw_input()
        except Exception as ex:
            print(ex)
        finally:
            # Force an exists call to succeed by resetting the status
            blob_client.response_callback = response_callback

    # Clean up resources
    blob_client.delete_container(container_name)


'''
RequestCompleted Event handler
If it's not pointing at the secondary, let if go through. It was either successful,
or it failed with a non-retryable event.
If it's pointing at the secondary, increment the read count.
If the number of reads has hit the threshold of how many reads you want to do against the secondary,
before you switch back to primary, switch back and reset the secondary_read_count
'''


def response_callback(response):
    global secondary_read_count
    if blob_client.location_mode == LocationMode.SECONDARY:

        # You're reading the secondary. Let it read the secondary [secondaryThreshold] times,
        # then switch back to the primary and see if it is available now.
        secondary_read_count += 1
        if secondary_read_count >= secondary_threshold:
            blob_client.location_mode = LocationMode.PRIMARY
            secondary_read_count = 0


'''
Retry Event handler
If it has retried more times than allowed, and it's not already pointed to the secondary,
flip it to the secondary and reset the retry count
If it has retried more times than allowed, and it's already pointed to the secondary throw an exception
'''


def retry_callback(retry_context):
    global retry_count
    retry_count = retry_context.count
    sys.stdout.write("\nRetrying event because of failure reading the primary. RetryCount= {0}".format(retry_count))
    sys.stdout.flush()

    # Check if we have more than n-retries in which case switch to secondary
    if retry_count >= retry_threshold:

        # Check to see if we can fail over to secondary.
        if blob_client.location_mode != LocationMode.SECONDARY:
            blob_client.location_mode = LocationMode.SECONDARY
            retry_count = 0
        else:
            raise Exception("Both primary and secondary are unreachable. "
                            "Check your application's network connection.")


if __name__ == '__main__':
    print("Azure storage Circuit Breaker Sample \n")
    try:
        run_circuit_breaker()
    except Exception as e:
        print("Error thrown = {0}".format(e))
    sys.stdout.write("\nPress any key to exit.")
    sys.stdout.flush()
    raw_input()
