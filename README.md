# Azure Search Index Backup and Restore

The purpose of this tool is to help with extraction of content from an Azure Search index and restoration to a new index.

## Important - Please Read

Search indexes are different from other datastores in that it is really hard to extract all content from the store.  Due to the way that search indexes are constantly ranking and scoring results, paging through search results or even using continuation tokes as this tool does has the possibility of missing data during data extraction.  As an example, lets say you search for all documents, and there is a document with ID 101 that is part of page 5 of the search results.  As you start extracting data from page to page as you move from page 4 to page 5, it is possible that now ID 101 is actually now part of page 4, meaning that when you look at page 5, it is no longer there and you just missed that document.

For that reason, this tool also includes a scan of the ID's of the keys extracted to that which is in the actual index to make sure that nothing was missed.  This extra step can be a lengthy process, but does help to reduce the chance of missed data.
