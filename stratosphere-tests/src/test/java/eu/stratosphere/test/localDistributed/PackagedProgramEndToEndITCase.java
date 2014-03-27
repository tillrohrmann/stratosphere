/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.test.localDistributed;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.client.RemoteExecutor;
import eu.stratosphere.client.localDistributed.LocalDistributedExecutor;
import eu.stratosphere.example.java.record.connectedcomponents.WorksetConnectedComponents;
import eu.stratosphere.example.java.record.wordcount.WordCount;
import eu.stratosphere.test.testdata.ConnectedComponentsData;
import eu.stratosphere.test.testdata.KMeansData;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.AbstractTestBase;
import eu.stratosphere.util.LogUtils;

// When the API changes WordCountForTest needs to be rebuilt and the WordCountForTest.jar in resources needs
// to be replaced with the new one.

public class PackagedProgramEndToEndITCase {

	static {
		LogUtils.initializeDefaultTestConsoleLogger();
	}

	@Test
	public void testLocalDistributedExecutorWithWordCount() {

		LocalDistributedExecutor lde = new LocalDistributedExecutor();

		try {
			// set up the files
			File inFile = File.createTempFile("wctext", ".in");
			File outFile = File.createTempFile("wctext", ".out");
			inFile.deleteOnExit();
			outFile.deleteOnExit();
			
			FileWriter fw = new FileWriter(inFile);
			fw.write(WordCountData.TEXT);
			fw.close();
			
			// run WordCount
			WordCount wc = new WordCount();

			lde.start(2);
			lde.run(wc.getPlan("4", inFile.toURI().toString(), outFile.toURI().toString()));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} finally {
			try {
				lde.stop();
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail(e.getMessage());
			}
		}
	}
	
	@Test
	public void testExternalKMeans() {
		LocalDistributedExecutor lde = new LocalDistributedExecutor();
		try {
			// set up the files
			File points = File.createTempFile("kmeans_points", ".in");
			File clusters = File.createTempFile("kmeans_clusters", ".in");
			File outFile = File.createTempFile("kmeans_result", ".out");
			points.deleteOnExit();
			clusters.deleteOnExit();
			outFile.deleteOnExit();
			outFile.delete();

			FileWriter fwPoints = new FileWriter(points);
			fwPoints.write(KMeansData.DATAPOINTS);
			fwPoints.close();

			FileWriter fwClusters = new FileWriter(clusters);
			fwClusters.write(KMeansData.INITIAL_CENTERS);
			fwClusters.close();

			URL jarFileURL = getClass().getResource("/KMeansForTest.jar");
			String jarPath = jarFileURL.getFile();

			// run WordCount

			lde.start(2);
			RemoteExecutor ex = new RemoteExecutor("localhost", 6498, Collections.<String>emptyList());

			ex.executeJar(jarPath,
					"eu.stratosphere.examples.scala.testing.KMeansForTest",
					new String[] {"4",
							points.toURI().toString(),
							clusters.toURI().toString(),
							outFile.toURI().toString(),
							"20"});
			
			
			ArrayList<String> resultLines = new ArrayList<String>();
			
			BufferedReader[] readers = AbstractTestBase.getResultReader(outFile.toURI().toString(), false);
			for (BufferedReader reader : readers) {
				String s = null;
				while ((s = reader.readLine()) != null) {
					resultLines.add(s);
				}
			}
			
			KMeansData.checkResultsWithDelta(KMeansData.CENTERS_AFTER_20_ITERATIONS_DOUBLE_DIGIT, resultLines, 0.1f);
			
			points.delete();
			clusters.delete();
			outFile.delete();

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} finally {
			try {
				lde.stop();
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail(e.getMessage());
			}
		}
	}

	@Test
	public void testLocalDistributedExecutorWithConnectedComponents() {
		LocalDistributedExecutor lde = new LocalDistributedExecutor();

		final long SEED = 0xBADC0FFEEBEEFL;

		final int NUM_VERTICES = 1000;

		final int NUM_EDGES = 10000;

		try {
			// set up the files
			File verticesFile = File.createTempFile("vertices", ".txt");
			File edgesFile = File.createTempFile("edges", ".txt");
			File resultFile = File.createTempFile("results", ".txt");

			verticesFile.deleteOnExit();
			edgesFile.deleteOnExit();
			resultFile.deleteOnExit();

			FileWriter verticesWriter = new FileWriter(verticesFile);
			verticesWriter.write(ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
			verticesWriter.close();

			FileWriter edgesWriter = new FileWriter(edgesFile);
			edgesWriter.write(ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
			edgesWriter.close();

			int dop = 4;
			String verticesPath = verticesFile.toURI().toString();
			String edgesPath = edgesFile.toURI().toString();
			String resultPath = resultFile.toURI().toString();
			int maxIterations = 100;

			String[] params = { String.valueOf(dop) , verticesPath, edgesPath, resultPath, String.valueOf(maxIterations) };

			WorksetConnectedComponents cc = new WorksetConnectedComponents();
			lde.start(2);
			lde.run(cc.getPlan(params));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} finally {
			try {
				lde.stop();
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail(e.getMessage());
			}
		}
	}
}