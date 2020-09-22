# Backup and Restore an Azure Cognitive Search Index using Python (Notebook)

The purpose of this tool is to help with extraction of content from an Azure Search index and restoration to a new index during the development phase.

## Important - Please Read
Search indexes are different from other datastores in that it is really hard to extract all content from the store. Due to the way that search indexes are constantly ranking and scoring results, paging through search results or even using continuation tokes as this tool does has the possibility of missing data during data extraction. As an example, lets say you search for all documents, and there is a document with ID 101 that is part of page 5 of the search results. As you start extracting data from page to page as you move from page 4 to page 5, it is possible that now ID 101 is actually now part of page 4, meaning that when you look at page 5, it is no longer there and you just missed that document.

For that reason, this tool keeps a count of the ID's of the keys extracted and will do a comparison to the count of documents in the Azure Search index to make sure they match. Although this does not provide a perfect solution, it does help reduce the chance of missing data.

Also, as an extra precaution, it is best if there are no changes being made and the search index is in a steady state during this extraction phase.
