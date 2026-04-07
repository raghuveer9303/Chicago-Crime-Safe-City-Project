import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import TopNav from "../components/TopNav";

export default function AboutPage() {
  return (
    <div className="min-h-screen bg-background">
      <TopNav />
      <main className="container max-w-4xl py-12 space-y-8">
        <div>
          <h1 className="text-4xl font-bold tracking-tight mb-4">About the Project</h1>
          <p className="text-lg text-muted-foreground">
            The Chicago Civic Risk Atlas aims to provide public-safety analytics and situational awareness
            through real-time streaming infrastructure and predictive models.
          </p>
        </div>

        {/* Architecture Block */}
        <Card>
          <CardHeader>
            <CardTitle>System Architecture</CardTitle>
            <CardDescription>End-to-end real-time data ingestion to ML predictions</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-col md:flex-row items-center justify-between gap-4 p-6 bg-muted/30 rounded-lg border">
              <div className="flex-1 text-center w-full">
                <div className="bg-primary text-primary-foreground p-3 rounded-md font-semibold text-sm">Data API</div>
                <p className="text-xs text-muted-foreground mt-2">Chicago Data Portal</p>
              </div>
              <div className="text-muted-foreground font-bold hidden md:block">→</div>
              <div className="text-muted-foreground font-bold md:hidden">↓</div>
              <div className="flex-1 text-center w-full">
                <div className="bg-card border p-3 rounded-md font-semibold text-sm">Broker</div>
                <p className="text-xs text-muted-foreground mt-2">Apache Kafka</p>
              </div>
              <div className="text-muted-foreground font-bold hidden md:block">→</div>
              <div className="text-muted-foreground font-bold md:hidden">↓</div>
              <div className="flex-1 text-center w-full">
                <div className="bg-secondary text-secondary-foreground border p-3 rounded-md font-semibold text-sm">Stream ML</div>
                <p className="text-xs text-muted-foreground mt-2">PySpark + LSTM</p>
              </div>
              <div className="text-muted-foreground font-bold hidden md:block">→</div>
              <div className="text-muted-foreground font-bold md:hidden">↓</div>
              <div className="flex-1 text-center w-full">
                <div className="bg-card border p-3 rounded-md font-semibold text-sm">Database</div>
                <p className="text-xs text-muted-foreground mt-2">PostgreSQL</p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Modeling & Features Block */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle>Core Metrics</CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="list-disc pl-5 space-y-2 text-muted-foreground">
                <li><strong className="text-foreground">Crime Probability:</strong> Likelihood of incidents within a specific hexagonal grid (H3).</li>
                <li><strong className="text-foreground">Risk Index:</strong> Normalized score aggregating recent temporal trends.</li>
                <li><strong className="text-foreground">Spatial Heatmaps:</strong> Visual density of filtered crime categories across districts.</li>
              </ul>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Machine Learning Process</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <p className="text-muted-foreground text-sm">
                Instead of simple heuristics, the predictive pipeline leverages an <strong className="text-foreground">LSTM (Long Short-Term Memory)</strong> neural network. This allows the system to seamlessly evaluate sequential historical crime data to forecast emerging hotspots.
              </p>
              <div>
                <h4 className="text-sm font-semibold mb-2">Key Variables Used:</h4>
                <div className="flex flex-wrap gap-2">
                  {["latitude", "longitude", "primary_type", "location_description", "time_of_day", "day_of_week"].map(feat => (
                    <span key={feat} className="bg-muted px-2 py-1 rounded-full text-xs text-muted-foreground">
                      {feat}
                    </span>
                  ))}
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Author</CardTitle>
            <CardDescription>Raghuveer Venkatesh</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-muted-foreground">
              Built by Raghuveer Venkatesh. This application acts as a showcase of end-to-end data pipelines, streaming infrastructure, and frontend data visualization.
            </p>
            <div className="flex gap-4">
              <a 
                href="https://raghuveervenkatesh.us" 
                target="_blank" 
                rel="noreferrer"
                className="text-primary hover:underline font-medium"
              >
                raghuveervenkatesh.us
              </a>
              <span className="text-muted-foreground">•</span>
              <a 
                href="https://www.linkedin.com/in/raghuveer-venkatesh/" 
                target="_blank" 
                rel="noreferrer"
                className="text-primary hover:underline font-medium"
              >
                LinkedIn Profile
              </a>
            </div>
          </CardContent>
        </Card>
      </main>
    </div>
  );
}
